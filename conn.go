// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

//JoinHostPort is a utility to return a address string that can be used
//gocql.Conn to form a connection with a host.
func JoinHostPort(addr string, port int) string {
	addr = strings.TrimSpace(addr)
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	return addr
}

type Authenticator interface {
	Challenge(req []byte) (resp []byte, auth Authenticator, err error)
	Success(data []byte) error
}

type PasswordAuthenticator struct {
	Username string
	Password string
}

func (p PasswordAuthenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	if string(req) != "org.apache.cassandra.auth.PasswordAuthenticator" {
		return nil, nil, fmt.Errorf("unexpected authenticator %q", req)
	}
	resp := make([]byte, 2+len(p.Username)+len(p.Password))
	resp[0] = 0
	copy(resp[1:], p.Username)
	resp[len(p.Username)+1] = 0
	copy(resp[2+len(p.Username):], p.Password)
	return resp, nil, nil
}

func (p PasswordAuthenticator) Success(data []byte) error {
	return nil
}

type SslOptions struct {
	CertPath string
	KeyPath  string
	CaPath   string //optional depending on server config
	// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this on
	// This option is basically the inverse of InSecureSkipVerify
	// See InSecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info
	EnableHostVerification bool
}

type ConnConfig struct {
	ProtoVersion  int
	CQLVersion    string
	Timeout       time.Duration
	NumStreams    int
	Compressor    Compressor
	Authenticator Authenticator
	Keepalive     time.Duration
	tlsConfig     *tls.Config
}

// Conn is a single connection to a Cassandra node. It can be used to execute
// queries, but users are usually advised to use a more reliable, higher
// level API.
type Conn struct {
	conn    net.Conn
	r       *bufio.Reader
	timeout time.Duration

	headerBuf []byte

	uniq  chan int
	calls []callReq

	pool            ConnectionPool
	compressor      Compressor
	auth            Authenticator
	addr            string
	version         uint8
	currentKeyspace string
	started         bool

	closedMu sync.RWMutex
	isClosed bool
}

// Connect establishes a connection to a Cassandra node.
// You must also call the Serve method before you can execute any queries.
func Connect(addr string, cfg ConnConfig, pool ConnectionPool) (*Conn, error) {
	var (
		err  error
		conn net.Conn
	)

	if cfg.tlsConfig != nil {
		// the TLS config is safe to be reused by connections but it must not
		// be modified after being used.
		if conn, err = tls.Dial("tcp", addr, cfg.tlsConfig); err != nil {
			return nil, err
		}
	} else if conn, err = net.DialTimeout("tcp", addr, cfg.Timeout); err != nil {
		return nil, err
	}

	// going to default to proto 2
	if cfg.ProtoVersion < protoVersion1 || cfg.ProtoVersion > protoVersion3 {
		log.Printf("unsupported protocol version: %d using 2\n", cfg.ProtoVersion)
		cfg.ProtoVersion = 2
	}

	headerSize := 8

	maxStreams := 128
	if cfg.ProtoVersion > protoVersion2 {
		maxStreams = 32768
		headerSize = 9
	}

	if cfg.NumStreams <= 0 || cfg.NumStreams > maxStreams {
		cfg.NumStreams = maxStreams
	}

	c := &Conn{
		conn:       conn,
		r:          bufio.NewReader(conn),
		uniq:       make(chan int, cfg.NumStreams),
		calls:      make([]callReq, cfg.NumStreams),
		timeout:    cfg.Timeout,
		version:    uint8(cfg.ProtoVersion),
		addr:       conn.RemoteAddr().String(),
		pool:       pool,
		compressor: cfg.Compressor,
		auth:       cfg.Authenticator,

		headerBuf: make([]byte, headerSize),
	}

	if cfg.Keepalive > 0 {
		c.setKeepalive(cfg.Keepalive)
	}

	for i := 0; i < cfg.NumStreams; i++ {
		c.calls[i].resp = make(chan error, 1)
		c.uniq <- i
	}

	go c.serve()

	if err := c.startup(&cfg); err != nil {
		conn.Close()
		return nil, err
	}
	c.started = true

	return c, nil
}

func (c *Conn) Write(p []byte) (int, error) {
	if c.timeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}

	return c.conn.Write(p)
}

func (c *Conn) Read(p []byte) (n int, err error) {
	const maxAttempts = 5

	for i := 0; i < maxAttempts; i++ {
		var nn int
		if c.timeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.timeout))
		}

		nn, err = io.ReadFull(c.r, p[n:])
		n += nn
		if err == nil {
			break
		}

		if verr, ok := err.(net.Error); !ok || !verr.Temporary() {
			break
		}
	}

	return
}

func (c *Conn) startup(cfg *ConnConfig) error {
	m := map[string]string{
		"CQL_VERSION": cfg.CQLVersion,
	}

	if c.compressor != nil {
		m["COMPRESSION"] = c.compressor.Name()
	}

	frame, err := c.exec(&writeStartupFrame{opts: m}, nil)
	if err != nil {
		return err
	}

	switch v := frame.(type) {
	case error:
		return v
	case *readyFrame:
		return nil
	case *authenticateFrame:
		return c.authenticateHandshake(v)
	default:
		return NewErrProtocol("Unknown type of response to startup frame: %s", v)
	}
}

func (c *Conn) authenticateHandshake(authFrame *authenticateFrame) error {
	if c.auth == nil {
		return fmt.Errorf("authentication required (using %q)", authFrame.class)
	}

	resp, challenger, err := c.auth.Challenge([]byte(authFrame.class))
	if err != nil {
		return err
	}

	req := &writeAuthResponseFrame{data: resp}

	for {
		frame, err := c.exec(req, nil)
		if err != nil {
			return err
		}

		switch v := frame.(type) {
		case error:
			return v
		case *authSuccessFrame:
			if challenger != nil {
				return challenger.Success(v.data)
			}
			return nil
		case *authChallengeFrame:
			resp, challenger, err = challenger.Challenge(v.data)
			if err != nil {
				return err
			}

			req = &writeAuthResponseFrame{
				data: resp,
			}
		default:
			return fmt.Errorf("unknown frame response during authentication: %v", v)
		}
	}
}

// Serve starts the stream multiplexer for this connection, which is required
// to execute any queries. This method runs as long as the connection is
// open and is therefore usually called in a separate goroutine.
func (c *Conn) serve() {
	var (
		err error
	)

	for {
		err = c.recv()
		if err != nil {
			break
		}
	}

	c.Close()
	for id := 0; id < len(c.calls); id++ {
		req := &c.calls[id]
		// we need to send the error to all waiting queries, put the state
		// of this conn into not active so that it can not execute any queries.
		select {
		case req.resp <- err:
		default:
		}

		close(req.resp)
	}

	if c.started {
		c.pool.HandleError(c, err, true)
	}
}

func (c *Conn) recv() error {
	// not safe for concurrent reads

	// read a full header, ignore timeouts, as this is being ran in a loop
	// TODO: TCP level deadlines? or just query level deadlines?
	if c.timeout > 0 {
		c.conn.SetReadDeadline(time.Time{})
	}

	// were just reading headers over and over and copy bodies
	head, err := readHeader(c.r, c.headerBuf)
	if err != nil {
		return err
	}

	call := &c.calls[head.stream]
	err = call.framer.readFrame(&head)
	if err != nil {
		return err
	}

	// once we get to here we know that the caller must be waiting and that there
	// is no error.
	call.resp <- nil
	c.uniq <- head.stream

	return nil
}

type callReq struct {
	// could use a waitgroup but this allows us to do timeouts on the read/send
	resp   chan error
	framer *framer
}

func (c *Conn) exec(req frameWriter, tracer Tracer) (frame, error) {
	// TODO: move tracer onto conn
	stream := <-c.uniq

	call := &c.calls[stream]
	// resp is basically a waiting semaphore protecting the framer
	framer := newFramer(c, c, c.compressor, c.version)
	call.framer = framer

	if tracer != nil {
		framer.trace()
	}

	err := req.writeFrame(framer, stream)
	if err != nil {
		return nil, err
	}

	err = <-call.resp
	if err != nil {
		return nil, err
	}

	if v := framer.header.version.version(); v != c.version {
		return nil, NewErrProtocol("unexpected protocol version in response: got %d expected %d", v, c.version)
	}

	frame, err := framer.parseFrame()
	if err != nil {
		return nil, err
	}

	if len(framer.traceID) > 0 {
		tracer.Trace(framer.traceID)
	}

	framerPool.Put(framer)
	call.framer = nil

	return frame, nil
}

func (c *Conn) prepareStatement(stmt string, trace Tracer) (*resultPreparedFrame, error) {
	stmtsLRU.Lock()
	if stmtsLRU.lru == nil {
		initStmtsLRU(defaultMaxPreparedStmts)
	}

	stmtCacheKey := c.addr + c.currentKeyspace + stmt

	if val, ok := stmtsLRU.lru.Get(stmtCacheKey); ok {
		stmtsLRU.Unlock()
		flight := val.(*inflightPrepare)
		flight.wg.Wait()
		return flight.info, flight.err
	}

	flight := new(inflightPrepare)
	flight.wg.Add(1)
	stmtsLRU.lru.Add(stmtCacheKey, flight)
	stmtsLRU.Unlock()

	prep := &writePrepareFrame{
		statement: stmt,
	}

	resp, err := c.exec(prep, trace)
	if err != nil {
		flight.err = err
		flight.wg.Done()
		return nil, err
	}

	switch x := resp.(type) {
	case *resultPreparedFrame:
		flight.info = x
	case error:
		flight.err = x
	default:
		flight.err = NewErrProtocol("Unknown type in response to prepare frame: %s", x)
	}
	flight.wg.Done()

	if flight.err != nil {
		stmtsLRU.Lock()
		stmtsLRU.lru.Remove(stmtCacheKey)
		stmtsLRU.Unlock()
	}

	return flight.info, flight.err
}

func (c *Conn) executeQuery(qry *Query) *Iter {
	params := queryParams{
		consistency: qry.cons,
	}

	// TODO: Add DefaultTimestamp, SerialConsistency
	if len(qry.pageState) > 0 {
		params.pagingState = qry.pageState
	}
	if qry.pageSize > 0 {
		params.pageSize = qry.pageSize
	}

	var frame frameWriter
	if qry.shouldPrepare() {
		// Prepare all DML queries. Other queries can not be prepared.
		info, err := c.prepareStatement(qry.stmt, qry.trace)
		if err != nil {
			return &Iter{err: err}
		}

		var values []interface{}

		if qry.binding == nil {
			values = qry.values
		} else {
			binding := &QueryInfo{
				Id:   info.preparedID,
				Args: info.reqMeta.columns,
				Rval: info.respMeta.columns,
			}

			values, err = qry.binding(binding)
			if err != nil {
				return &Iter{err: err}
			}
		}

		if len(values) != len(info.reqMeta.columns) {
			return &Iter{err: ErrQueryArgLength}
		}
		params.values = make([]queryValues, len(values))
		for i := 0; i < len(values); i++ {
			val, err := Marshal(info.reqMeta.columns[i].TypeInfo, values[i])
			if err != nil {
				return &Iter{err: err}
			}

			v := &params.values[i]
			v.value = val
			// TODO: handle query binding names
		}

		frame = &writeExecuteFrame{
			preparedID: info.preparedID,
			params:     params,
		}
	} else {
		frame = &writeQueryFrame{
			statement: qry.stmt,
			params:    params,
		}
	}

	resp, err := c.exec(frame, qry.trace)
	if err != nil {
		return &Iter{err: err}
	}

	switch x := resp.(type) {
	case *resultVoidFrame:
		return &Iter{}
	case *resultRowsFrame:
		iter := &Iter{
			meta: x.meta,
			rows: x.rows,
		}

		if len(x.meta.pagingState) > 0 {
			iter.next = &nextIter{
				qry: *qry,
				pos: int((1 - qry.prefetch) * float64(len(iter.rows))),
			}

			iter.next.qry.pageState = x.meta.pagingState
			if iter.next.pos < 1 {
				iter.next.pos = 1
			}
		}

		return iter
	case *resultKeyspaceFrame, *resultSchemaChangeFrame:
		return &Iter{}
	case *RequestErrUnprepared:
		stmtsLRU.Lock()
		stmtCacheKey := c.addr + c.currentKeyspace + qry.stmt
		if _, ok := stmtsLRU.lru.Get(stmtCacheKey); ok {
			stmtsLRU.lru.Remove(stmtCacheKey)
			stmtsLRU.Unlock()
			return c.executeQuery(qry)
		}
		stmtsLRU.Unlock()
		return &Iter{err: x}
	case error:
		return &Iter{err: x}
	default:
		return &Iter{err: NewErrProtocol("Unknown type in response to execute query: %s", x)}
	}
}

func (c *Conn) Pick(qry *Query) *Conn {
	if c.Closed() {
		return nil
	}
	return c
}

func (c *Conn) Closed() bool {
	c.closedMu.RLock()
	closed := c.isClosed
	c.closedMu.RUnlock()
	return closed
}

func (c *Conn) Close() {
	c.closedMu.Lock()
	if c.isClosed {
		c.closedMu.Unlock()
		return
	}
	c.isClosed = true
	c.closedMu.Unlock()

	c.conn.Close()
}

func (c *Conn) Address() string {
	return c.addr
}

func (c *Conn) AvailableStreams() int {
	return len(c.uniq)
}

func (c *Conn) UseKeyspace(keyspace string) error {
	q := &writeQueryFrame{statement: `USE "` + keyspace + `"`}
	q.params.consistency = Any

	resp, err := c.exec(q, nil)
	if err != nil {
		return err
	}

	switch x := resp.(type) {
	case *resultKeyspaceFrame:
	case error:
		return x
	default:
		return NewErrProtocol("unknown frame in response to USE: %v", x)
	}

	c.currentKeyspace = keyspace

	return nil
}

func (c *Conn) executeBatch(batch *Batch) error {
	if c.version == protoVersion1 {
		return ErrUnsupported
	}

	n := len(batch.Entries)
	req := &writeBatchFrame{
		typ:         batch.Type,
		statements:  make([]batchStatment, n),
		consistency: batch.Cons,
	}

	stmts := make(map[string]string)

	for i := 0; i < n; i++ {
		entry := &batch.Entries[i]
		b := &req.statements[i]
		if len(entry.Args) > 0 || entry.binding != nil {
			info, err := c.prepareStatement(entry.Stmt, nil)
			if err != nil {
				return err
			}

			var args []interface{}
			if entry.binding == nil {
				args = entry.Args
			} else {
				binding := &QueryInfo{
					Id:   info.preparedID,
					Args: info.reqMeta.columns,
					Rval: info.respMeta.columns,
				}
				args, err = entry.binding(binding)
				if err != nil {
					return err
				}
			}

			if len(args) != len(info.reqMeta.columns) {
				return ErrQueryArgLength
			}

			b.preparedID = info.preparedID
			stmts[string(info.preparedID)] = entry.Stmt

			b.values = make([]queryValues, len(info.reqMeta.columns))

			for j := 0; j < len(info.reqMeta.columns); j++ {
				val, err := Marshal(info.reqMeta.columns[j].TypeInfo, args[j])
				if err != nil {
					return err
				}

				b.values[j].value = val
				// TODO: add names
			}
		} else {
			b.statement = entry.Stmt
		}
	}

	// TODO: should batch support tracing?
	resp, err := c.exec(req, nil)
	if err != nil {
		return err
	}

	switch x := resp.(type) {
	case *resultVoidFrame:
		return nil
	case *RequestErrUnprepared:
		stmt, found := stmts[string(x.StatementId)]
		if found {
			stmtsLRU.Lock()
			stmtsLRU.lru.Remove(c.addr + c.currentKeyspace + stmt)
			stmtsLRU.Unlock()
		}
		if found {
			return c.executeBatch(batch)
		} else {
			return x
		}
	case error:
		return x
	default:
		return NewErrProtocol("Unknown type in response to batch statement: %s", x)
	}
}

func (c *Conn) setKeepalive(d time.Duration) error {
	if tc, ok := c.conn.(*net.TCPConn); ok {
		err := tc.SetKeepAlivePeriod(d)
		if err != nil {
			return err
		}

		return tc.SetKeepAlive(true)
	}

	return nil
}

type inflightPrepare struct {
	info *resultPreparedFrame
	err  error
	wg   sync.WaitGroup
}

var (
	ErrQueryArgLength = errors.New("query argument length mismatch")
)
