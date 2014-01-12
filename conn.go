// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/snappy-go/snappy"
)

const defaultFrameSize = 4096
const flagResponse = 0x80
const maskVersion = 0x7F

type Cluster interface {
	//HandleAuth(addr, method string) ([]byte, Challenger, error)
	HandleError(conn *Conn, err error, closed bool)
	HandleKeyspace(conn *Conn, keyspace string)
	// Authenticate(addr string)
}

/* type Challenger interface {
	Challenge(data []byte) ([]byte, error)
} */

type ConnConfig struct {
	ProtoVersion int
	CQLVersion   string
	Timeout      time.Duration
	NumStreams   int
	Compressor   Compressor
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

	prepMu sync.Mutex
	prep   map[string]*inflightPrepare

	cluster    Cluster
	compressor Compressor
	addr       string
	version    uint8
	hostID     UUID
}

// Connect establishes a connection to a Cassandra node.
// You must also call the Serve method before you can execute any queries.
func Connect(addr string, cfg ConnConfig, cluster Cluster) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, cfg.Timeout)
	if err != nil {
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
		prep:       make(map[string]*inflightPrepare),
		timeout:    cfg.Timeout,
		version:    uint8(cfg.ProtoVersion),
		addr:       conn.RemoteAddr().String(),
		cluster:    cluster,
		compressor: cfg.Compressor,
	}
	for i := 0; i < cap(c.uniq); i++ {
		c.uniq <- uint8(i)
	}

	if err := c.startup(&cfg); err != nil {
		return nil, err
	}

	go c.serve()

	return c, nil
}

func (c *Conn) startup(cfg *ConnConfig) error {
	req := &startupFrame{
		CQLVersion: cfg.CQLVersion,
	}
	if c.compressor != nil {
		req.Compression = c.compressor.Name()
	}
	resp, err := c.execSimple(req)
	if err != nil {
		return err
	}
	switch x := resp.(type) {
	case readyFrame:
	case error:
		return x
	default:
		return ErrProtocol
	}
	return nil
}

// Serve starts the stream multiplexer for this connection, which is required
// to execute any queries. This method runs as long as the connection is
// open and is therefore usually called in a separate goroutine.
func (c *Conn) serve() {
	for {
		resp, err := c.recv()
		if err != nil {
			break
		}
		c.dispatch(resp)
	}

	c.conn.Close()
	for id := 0; id < len(c.calls); id++ {
		req := &c.calls[id]
		if atomic.LoadInt32(&req.active) == 1 {
			req.resp <- callResp{nil, ErrProtocol}
		}
	}
	c.cluster.HandleError(c, ErrProtocol, true)
}

func (c *Conn) recv() (frame, error) {
	resp := make(frame, headerSize, headerSize+512)
	c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	n, last, pinged := 0, 0, false
	for n < len(resp) {
		nn, err := c.r.Read(resp[n:])
		n += nn
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
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
				return nil, ErrProtocol
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
		c.conn.Close()
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

	if n, err := c.conn.Write(req); err != nil {
		c.conn.Close()
		if n > 0 {
			return nil, ErrProtocol
		}
		return nil, ErrUnavailable
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

func (c *Conn) prepareStatement(stmt string, trace Tracer) (*queryInfo, error) {
	c.prepMu.Lock()
	flight := c.prep[stmt]
	if flight != nil {
		c.prepMu.Unlock()
		flight.wg.Wait()
		return flight.info, flight.err
	}

	flight = new(inflightPrepare)
	flight.wg.Add(1)
	c.prep[stmt] = flight
	c.prepMu.Unlock()

	resp, err := c.exec(&prepareFrame{Stmt: stmt}, trace)
	if err != nil {
		flight.err = err
	} else {
		switch x := resp.(type) {
		case resultPreparedFrame:
			flight.info = &queryInfo{
				id:   x.PreparedId,
				args: x.Values,
			}
		case error:
			flight.err = x
		default:
			flight.err = ErrProtocol
		}
	}

	flight.wg.Done()

	if err != nil {
		c.prepMu.Lock()
		delete(c.prep, stmt)
		c.prepMu.Unlock()
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
	if len(qry.values) > 0 {
		info, err := c.prepareStatement(qry.stmt, qry.trace)
		if err != nil {
			return &Iter{err: err}
		}
		op.Prepared = info.id
		op.Values = make([][]byte, len(qry.values))
		for i := 0; i < len(qry.values); i++ {
			val, err := Marshal(info.args[i].TypeInfo, qry.values[i])
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
		c.cluster.HandleKeyspace(c, x.Keyspace)
		return &Iter{}
	case errorFrame:
		if x.Code == errUnprepared && len(qry.values) > 0 {
			c.prepMu.Lock()
			if val, ok := c.prep[qry.stmt]; ok && val != nil {
				delete(c.prep, qry.stmt)
				c.prepMu.Unlock()
				return c.executeQuery(qry)
			}
			c.prepMu.Unlock()
			return &Iter{err: x, errFrame: &x}
		} else {
			return &Iter{err: x, errFrame: &x}
		}
	case error:
		return &Iter{err: x}
	default:
		return &Iter{err: ErrProtocol}
	}
}

func (c *Conn) Pick(qry *Query) *Conn {
	return c
}

func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) Address() string {
	return c.addr
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
		return ErrProtocol
	}
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
	for i := 0; i < len(batch.Entries); i++ {
		entry := &batch.Entries[i]
		var info *queryInfo
		if len(entry.Args) > 0 {
			var err error
			info, err = c.prepareStatement(entry.Stmt, nil)
			if err != nil {
				return err
			}
			f.writeByte(1)
			f.writeShortBytes(info.id)
		} else {
			f.writeByte(0)
			f.writeLongString(entry.Stmt)
		}
		f.writeShort(uint16(len(entry.Args)))
		for j := 0; j < len(entry.Args); j++ {
			val, err := Marshal(info.args[j].TypeInfo, entry.Args[j])
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
	case error:
		return x
	default:
		return ErrProtocol
	}
}

func (c *Conn) decodeFrame(f frame, trace Tracer) (rval interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && e == ErrProtocol {
				err = e
				return
			}
			panic(r)
		}
	}()
	if len(f) < headerSize || (f[0] != c.version|flagResponse) {
		return nil, ErrProtocol
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
			return nil, ErrProtocol
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
			values, _ := f.readMetaData()
			return resultPreparedFrame{id, values}, nil
		case resultKindSchemaChanged:
			return resultVoidFrame{}, nil
		default:
			return nil, ErrProtocol
		}
	case opSupported:
		return supportedFrame{}, nil
	case opError:
		code := f.readInt()
		msg := f.readString()
		return errorFrame{code, msg}, nil
	default:
		return nil, ErrProtocol
	}
}

type queryInfo struct {
	id   []byte
	args []ColumnInfo
	rval []ColumnInfo
}

type callReq struct {
	active int32
	resp   chan callResp
}

type callResp struct {
	buf frame
	err error
}

type Compressor interface {
	Name() string
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}

type inflightPrepare struct {
	info *queryInfo
	err  error
	wg   sync.WaitGroup
}

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. The snappy compression algorithm
// aims for very high speeds and reasonable compression.
type SnappyCompressor struct{}

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) Encode(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data)
}

func (s SnappyCompressor) Decode(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
