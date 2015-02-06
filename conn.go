// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultFrameSize = 4096
const flagResponse = 0x80
const maskVersion = 0x7F

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
	SslOpts       *SslOptions
}

// Conn is a single connection to a Cassandra node. It can be used to execute
// queries, but users are usually advised to use a more reliable, higher
// level API.
type Conn struct {
	conn    net.Conn
	r       *bufio.Reader
	timeout time.Duration

	uniq  chan uint8
	calls []callReq
	nwait int32

	pool            ConnectionPool
	compressor      Compressor
	auth            Authenticator
	addr            string
	version         uint8
	currentKeyspace string

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

	if cfg.SslOpts != nil {
		certPool := x509.NewCertPool()
		//ca cert is optional
		if cfg.SslOpts.CaPath != "" {
			pem, err := ioutil.ReadFile(cfg.SslOpts.CaPath)
			if err != nil {
				return nil, err
			}
			if !certPool.AppendCertsFromPEM(pem) {
				return nil, errors.New("Failed parsing or appending certs")
			}
		}
		mycert, err := tls.LoadX509KeyPair(cfg.SslOpts.CertPath, cfg.SslOpts.KeyPath)
		if err != nil {
			return nil, err
		}
		config := tls.Config{
			Certificates: []tls.Certificate{mycert},
			RootCAs:      certPool,
		}
		config.InsecureSkipVerify = !cfg.SslOpts.EnableHostVerification
		if conn, err = tls.Dial("tcp", addr, &config); err != nil {
			return nil, err
		}
	} else if conn, err = net.DialTimeout("tcp", addr, cfg.Timeout); err != nil {
		return nil, err
	}

	if cfg.NumStreams <= 0 || cfg.NumStreams > 128 {
		cfg.NumStreams = 128
	}
	if cfg.ProtoVersion != 1 && cfg.ProtoVersion != 2 {
		cfg.ProtoVersion = 2
	}
	c := &Conn{
		conn:       conn,
		r:          bufio.NewReader(conn),
		uniq:       make(chan uint8, cfg.NumStreams),
		calls:      make([]callReq, cfg.NumStreams),
		timeout:    cfg.Timeout,
		version:    uint8(cfg.ProtoVersion),
		addr:       conn.RemoteAddr().String(),
		pool:       pool,
		compressor: cfg.Compressor,
		auth:       cfg.Authenticator,
	}

	if cfg.Keepalive > 0 {
		c.setKeepalive(cfg.Keepalive)
	}

	for i := 0; i < cap(c.uniq); i++ {
		c.uniq <- uint8(i)
	}

	if err := c.startup(&cfg); err != nil {
		conn.Close()
		return nil, err
	}

	go c.serve()

	return c, nil
}

func (c *Conn) startup(cfg *ConnConfig) error {
	compression := ""
	if c.compressor != nil {
		compression = c.compressor.Name()
	}
	var req operation = &startupFrame{
		CQLVersion:  cfg.CQLVersion,
		Compression: compression,
	}
	var challenger Authenticator
	for {
		resp, err := c.execSimple(req)
		if err != nil {
			return err
		}
		switch x := resp.(type) {
		case readyFrame:
			return nil
		case error:
			return x
		case authenticateFrame:
			if c.auth == nil {
				return fmt.Errorf("authentication required (using %q)", x.Authenticator)
			}
			var resp []byte
			resp, challenger, err = c.auth.Challenge([]byte(x.Authenticator))
			if err != nil {
				return err
			}
			req = &authResponseFrame{resp}
		case authChallengeFrame:
			if challenger == nil {
				return fmt.Errorf("authentication error (invalid challenge)")
			}
			var resp []byte
			resp, challenger, err = challenger.Challenge(x.Data)
			if err != nil {
				return err
			}
			req = &authResponseFrame{resp}
		case authSuccessFrame:
			if challenger != nil {
				return challenger.Success(x.Data)
			}
			return nil
		default:
			return NewErrProtocol("Unknown type of response to startup frame: %s", x)
		}
	}
}

// Serve starts the stream multiplexer for this connection, which is required
// to execute any queries. This method runs as long as the connection is
// open and is therefore usually called in a separate goroutine.
func (c *Conn) serve() {
	var (
		err  error
		resp frame
	)

	for {
		resp, err = c.recv()
		if err != nil {
			break
		}
		c.dispatch(resp)
	}

	c.Close()
	for id := 0; id < len(c.calls); id++ {
		req := &c.calls[id]
		if atomic.LoadInt32(&req.active) == 1 {
			req.resp <- callResp{nil, err}
		}
	}
	c.pool.HandleError(c, err, true)
}

func (c *Conn) recv() (frame, error) {
	resp := make(frame, headerSize, headerSize+512)
	c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	n, last, pinged := 0, 0, false
	for n < len(resp) {
		nn, err := c.r.Read(resp[n:])
		n += nn
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				if n > last {
					// we hit the deadline but we made progress.
					// simply extend the deadline
					c.conn.SetReadDeadline(time.Now().Add(c.timeout))
					last = n
				} else if n == 0 && !pinged {
					c.conn.SetReadDeadline(time.Now().Add(c.timeout))
					if atomic.LoadInt32(&c.nwait) > 0 {
						go c.ping()
						pinged = true
					}
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		if n == headerSize && len(resp) == headerSize {
			if resp[0] != c.version|flagResponse {
				return nil, NewErrProtocol("recv: Response protocol version does not match connection protocol version (%d != %d)", resp[0], c.version|flagResponse)
			}
			resp.grow(resp.Length())
		}
	}
	return resp, nil
}

func (c *Conn) execSimple(op operation) (interface{}, error) {
	f, err := op.encodeFrame(c.version, nil)
	f.setLength(len(f) - headerSize)
	if _, err := c.conn.Write([]byte(f)); err != nil {
		c.Close()
		return nil, err
	}
	if f, err = c.recv(); err != nil {
		return nil, err
	}
	return c.decodeFrame(f, nil)
}

func (c *Conn) exec(op operation, trace Tracer) (interface{}, error) {
	req, err := op.encodeFrame(c.version, nil)
	if err != nil {
		return nil, err
	}
	if trace != nil {
		req[1] |= flagTrace
	}
	if len(req) > headerSize && c.compressor != nil {
		body, err := c.compressor.Encode([]byte(req[headerSize:]))
		if err != nil {
			return nil, err
		}
		req = append(req[:headerSize], frame(body)...)
		req[1] |= flagCompress
	}
	req.setLength(len(req) - headerSize)

	id := <-c.uniq
	req[2] = id
	call := &c.calls[id]
	call.resp = make(chan callResp, 1)
	atomic.AddInt32(&c.nwait, 1)
	atomic.StoreInt32(&call.active, 1)

	if _, err := c.conn.Write(req); err != nil {
		c.uniq <- id
		c.Close()
		return nil, err
	}

	reply := <-call.resp
	call.resp = nil
	c.uniq <- id

	if reply.err != nil {
		return nil, reply.err
	}
	return c.decodeFrame(reply.buf, trace)
}

func (c *Conn) dispatch(resp frame) {
	id := int(resp[2])
	if id >= len(c.calls) {
		return
	}
	call := &c.calls[id]
	if !atomic.CompareAndSwapInt32(&call.active, 1, 0) {
		return
	}
	atomic.AddInt32(&c.nwait, -1)
	call.resp <- callResp{resp, nil}
}

func (c *Conn) ping() error {
	_, err := c.exec(&optionsFrame{}, nil)
	return err
}

func (c *Conn) prepareStatement(stmt string, trace Tracer) (*QueryInfo, error) {
	stmtsLRU.Lock()
	if stmtsLRU.lru == nil {
		initStmtsLRU(defaultMaxPreparedStmts)
	}

	stmtCacheKey := c.addr + c.currentKeyspace + stmt

	if val, ok := stmtsLRU.lru.Get(stmtCacheKey); ok {
		flight := val.(*inflightPrepare)
		stmtsLRU.Unlock()
		flight.wg.Wait()
		return flight.info, flight.err
	}

	flight := new(inflightPrepare)
	flight.wg.Add(1)
	stmtsLRU.lru.Add(stmtCacheKey, flight)
	stmtsLRU.Unlock()

	resp, err := c.exec(&prepareFrame{Stmt: stmt}, trace)
	if err != nil {
		flight.err = err
	} else {
		switch x := resp.(type) {
		case resultPreparedFrame:
			flight.info = &QueryInfo{
				Id:   x.PreparedId,
				Args: x.Arguments,
				Rval: x.ReturnValues,
			}
		case error:
			flight.err = x
		default:
			flight.err = NewErrProtocol("Unknown type in response to prepare frame: %s", x)
		}
		err = flight.err
	}

	flight.wg.Done()

	if err != nil {
		stmtsLRU.Lock()
		stmtsLRU.lru.Remove(stmtCacheKey)
		stmtsLRU.Unlock()
	}

	return flight.info, flight.err
}

func (c *Conn) executeQuery(qry *Query) *Iter {
	op := &queryFrame{
		Stmt:      qry.stmt,
		Cons:      qry.cons,
		PageSize:  qry.pageSize,
		PageState: qry.pageState,
	}
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
			values, err = qry.binding(info)
			if err != nil {
				return &Iter{err: err}
			}
		}

		if len(values) != len(info.Args) {
			return &Iter{err: ErrQueryArgLength}
		}
		op.Prepared = info.Id
		op.Values = make([][]byte, len(values))
		for i := 0; i < len(values); i++ {
			val, err := Marshal(info.Args[i].TypeInfo, values[i])
			if err != nil {
				return &Iter{err: err}
			}
			op.Values[i] = val
		}
	}
	resp, err := c.exec(op, qry.trace)
	if err != nil {
		return &Iter{err: err}
	}
	switch x := resp.(type) {
	case resultVoidFrame:
		return &Iter{}
	case resultRowsFrame:
		iter := &Iter{columns: x.Columns, rows: x.Rows}
		if len(x.PagingState) > 0 {
			iter.next = &nextIter{
				qry: *qry,
				pos: int((1 - qry.prefetch) * float64(len(iter.rows))),
			}
			iter.next.qry.pageState = x.PagingState
			if iter.next.pos < 1 {
				iter.next.pos = 1
			}
		}
		return iter
	case resultKeyspaceFrame:
		return &Iter{}
	case RequestErrUnprepared:
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
	resp, err := c.exec(&queryFrame{Stmt: `USE "` + keyspace + `"`, Cons: Any}, nil)
	if err != nil {
		return err
	}
	switch x := resp.(type) {
	case resultKeyspaceFrame:
	case error:
		return x
	default:
		return NewErrProtocol("Unknown type in response to USE: %s", x)
	}

	c.currentKeyspace = keyspace

	return nil
}

func (c *Conn) executeBatch(batch *Batch) error {
	if c.version == 1 {
		return ErrUnsupported
	}
	f := make(frame, headerSize, defaultFrameSize)
	f.setHeader(c.version, 0, 0, opBatch)
	f.writeByte(byte(batch.Type))
	f.writeShort(uint16(len(batch.Entries)))

	stmts := make(map[string]string)

	for i := 0; i < len(batch.Entries); i++ {
		entry := &batch.Entries[i]
		var info *QueryInfo
		var args []interface{}
		if len(entry.Args) > 0 || entry.binding != nil {
			var err error
			info, err = c.prepareStatement(entry.Stmt, nil)
			if err != nil {
				return err
			}

			if entry.binding == nil {
				args = entry.Args
			} else {
				args, err = entry.binding(info)
				if err != nil {
					return err
				}
			}

			if len(args) != len(info.Args) {
				return ErrQueryArgLength
			}

			stmts[string(info.Id)] = entry.Stmt
			f.writeByte(1)
			f.writeShortBytes(info.Id)
		} else {
			f.writeByte(0)
			f.writeLongString(entry.Stmt)
		}
		f.writeShort(uint16(len(args)))
		for j := 0; j < len(args); j++ {
			val, err := Marshal(info.Args[j].TypeInfo, args[j])
			if err != nil {
				return err
			}
			f.writeBytes(val)
		}
	}
	f.writeConsistency(batch.Cons)

	resp, err := c.exec(f, nil)
	if err != nil {
		return err
	}
	switch x := resp.(type) {
	case resultVoidFrame:
		return nil
	case RequestErrUnprepared:
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

func (c *Conn) decodeFrame(f frame, trace Tracer) (rval interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(ErrProtocol); ok {
				err = e
				return
			}
			panic(r)
		}
	}()
	if len(f) < headerSize {
		return nil, NewErrProtocol("Decoding frame: less data received than required for header: %d < %d", len(f), headerSize)
	} else if f[0] != c.version|flagResponse {
		return nil, NewErrProtocol("Decoding frame: response protocol version does not match connection protocol version (%d != %d)", f[0], c.version|flagResponse)
	}
	flags, op, f := f[1], f[3], f[headerSize:]
	if flags&flagCompress != 0 && len(f) > 0 && c.compressor != nil {
		if buf, err := c.compressor.Decode([]byte(f)); err != nil {
			return nil, err
		} else {
			f = frame(buf)
		}
	}
	if flags&flagTrace != 0 {
		if len(f) < 16 {
			return nil, NewErrProtocol("Decoding frame: length of frame less than 16 while tracing is enabled")
		}
		traceId := []byte(f[:16])
		f = f[16:]
		trace.Trace(traceId)
	}

	switch op {
	case opReady:
		return readyFrame{}, nil
	case opResult:
		switch kind := f.readInt(); kind {
		case resultKindVoid:
			return resultVoidFrame{}, nil
		case resultKindRows:
			columns, pageState := f.readMetaData()
			numRows := f.readInt()
			values := make([][]byte, numRows*len(columns))
			for i := 0; i < len(values); i++ {
				values[i] = f.readBytes()
			}
			rows := make([][][]byte, numRows)
			for i := 0; i < numRows; i++ {
				rows[i], values = values[:len(columns)], values[len(columns):]
			}
			return resultRowsFrame{columns, rows, pageState}, nil
		case resultKindKeyspace:
			keyspace := f.readString()
			return resultKeyspaceFrame{keyspace}, nil
		case resultKindPrepared:
			id := f.readShortBytes()
			args, _ := f.readMetaData()
			if c.version < 2 {
				return resultPreparedFrame{PreparedId: id, Arguments: args}, nil
			}
			rvals, _ := f.readMetaData()
			return resultPreparedFrame{PreparedId: id, Arguments: args, ReturnValues: rvals}, nil
		case resultKindSchemaChanged:
			return resultVoidFrame{}, nil
		default:
			return nil, NewErrProtocol("Decoding frame: unknown result kind %s", kind)
		}
	case opAuthenticate:
		return authenticateFrame{f.readString()}, nil
	case opAuthChallenge:
		return authChallengeFrame{f.readBytes()}, nil
	case opAuthSuccess:
		return authSuccessFrame{f.readBytes()}, nil
	case opSupported:
		return supportedFrame{}, nil
	case opError:
		return f.readError(), nil
	default:
		return nil, NewErrProtocol("Decoding frame: unknown op", op)
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

// QueryInfo represents the meta data associated with a prepared CQL statement.
type QueryInfo struct {
	Id   []byte
	Args []ColumnInfo
	Rval []ColumnInfo
}

type callReq struct {
	active int32
	resp   chan callResp
}

type callResp struct {
	buf frame
	err error
}

type inflightPrepare struct {
	info *QueryInfo
	err  error
	wg   sync.WaitGroup
}

var (
	ErrQueryArgLength = errors.New("query argument length mismatch")
)
