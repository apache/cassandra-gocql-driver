// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
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
}

// Conn is a single connection to a Cassandra node. It can be used to execute
// queries, but users are usually advised to use a more reliable, higher
// level API.
type Conn struct {
	conn    net.Conn
	timeout time.Duration

	uniq  chan uint8
	calls []callReq
	nwait int32

	prepMu sync.Mutex
	prep   map[string]*queryInfo

	cluster Cluster
	addr    string
	version uint8
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
		conn:    conn,
		uniq:    make(chan uint8, cfg.NumStreams),
		calls:   make([]callReq, cfg.NumStreams),
		prep:    make(map[string]*queryInfo),
		timeout: cfg.Timeout,
		version: uint8(cfg.ProtoVersion),
		addr:    conn.RemoteAddr().String(),
		cluster: cluster,
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
	req := make(frame, headerSize, defaultFrameSize)
	req.setHeader(c.version, 0, 0, opStartup)
	req.writeStringMap(map[string]string{
		"CQL_VERSION": cfg.CQLVersion,
	})
	resp, err := c.callSimple(req)
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
		nn, err := c.conn.Read(resp[n:])
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

func (c *Conn) callSimple(req frame) (interface{}, error) {
	req.setLength(len(req) - headerSize)
	if _, err := c.conn.Write(req); err != nil {
		c.conn.Close()
		return nil, err
	}
	buf, err := c.recv()
	if err != nil {
		return nil, err
	}
	return decodeFrame(buf)
}

func (c *Conn) call(req frame) (interface{}, error) {
	id := <-c.uniq
	req[2] = id

	call := &c.calls[id]
	call.resp = make(chan callResp, 1)
	atomic.AddInt32(&c.nwait, 1)
	atomic.StoreInt32(&call.active, 1)

	req.setLength(len(req) - headerSize)
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
	return decodeFrame(reply.buf)
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
	req := make(frame, headerSize)
	req.setHeader(c.version, 0, 0, opOptions)
	_, err := c.call(req)
	return err
}

func (c *Conn) prepareStatement(stmt string) (*queryInfo, error) {
	c.prepMu.Lock()
	info := c.prep[stmt]
	if info != nil {
		c.prepMu.Unlock()
		info.wg.Wait()
		return info, nil
	}
	info = new(queryInfo)
	info.wg.Add(1)
	c.prep[stmt] = info
	c.prepMu.Unlock()

	frame := make(frame, headerSize, defaultFrameSize)
	frame.setHeader(c.version, 0, 0, opPrepare)
	frame.writeLongString(stmt)
	frame.setLength(len(frame) - headerSize)

	resp, err := c.call(frame)
	if err != nil {
		return nil, err
	}
	switch x := resp.(type) {
	case resultPreparedFrame:
		info.id = x.PreparedId
		info.args = x.Values
		info.wg.Done()
	case error:
		return nil, x
	default:
		return nil, ErrProtocol
	}
	return info, nil
}

func (c *Conn) ExecuteQuery(qry *Query) (*Iter, error) {
	var info *queryInfo
	if len(qry.Args) > 0 {
		var err error
		info, err = c.prepareStatement(qry.Stmt)
		if err != nil {
			return nil, err
		}
	}
	req := make(frame, headerSize, defaultFrameSize)
	if info == nil {
		req.setHeader(c.version, 0, 0, opQuery)
		req.writeLongString(qry.Stmt)
		req.writeConsistency(qry.Cons)
		if c.version > 1 {
			req.writeByte(0)
		}
	} else {
		req.setHeader(c.version, 0, 0, opExecute)
		req.writeShortBytes(info.id)
		if c.version == 1 {
			req.writeShort(uint16(len(qry.Args)))
		} else {
			req.writeConsistency(qry.Cons)
			flags := uint8(0)
			if len(qry.Args) > 0 {
				flags |= flagQueryValues
			}
			req.writeByte(flags)
			if flags&flagQueryValues != 0 {
				req.writeShort(uint16(len(qry.Args)))
			}
		}
		for i := 0; i < len(qry.Args); i++ {
			val, err := Marshal(info.args[i].TypeInfo, qry.Args[i])
			if err != nil {
				return nil, err
			}
			req.writeBytes(val)
		}
		if c.version == 1 {
			req.writeConsistency(qry.Cons)
		}
	}
	resp, err := c.call(req)
	if err != nil {
		return nil, err
	}
	switch x := resp.(type) {
	case resultVoidFrame:
		return &Iter{}, nil
	case resultRowsFrame:
		return &Iter{columns: x.Columns, rows: x.Rows}, nil
	case resultKeyspaceFrame:
		c.cluster.HandleKeyspace(c, x.Keyspace)
		return &Iter{}, nil
	case error:
		return &Iter{err: x}, nil
	}
	return nil, ErrProtocol
}

func (c *Conn) ExecuteBatch(batch *Batch) error {
	if c.version == 1 {
		return ErrProtocol
	}
	frame := make(frame, headerSize, defaultFrameSize)
	frame.setHeader(c.version, 0, 0, opBatch)
	frame.writeByte(byte(batch.Type))
	frame.writeShort(uint16(len(batch.Entries)))
	for i := 0; i < len(batch.Entries); i++ {
		entry := &batch.Entries[i]
		var info *queryInfo
		if len(entry.Args) > 0 {
			var err error
			info, err = c.prepareStatement(entry.Stmt)
			if err != nil {
				return err
			}
			frame.writeByte(1)
			frame.writeShortBytes(info.id)
		} else {
			frame.writeByte(0)
			frame.writeLongString(entry.Stmt)
		}
		frame.writeShort(uint16(len(entry.Args)))
		for j := 0; j < len(entry.Args); j++ {
			val, err := Marshal(info.args[j].TypeInfo, entry.Args[j])
			if err != nil {
				return err
			}
			frame.writeBytes(val)
		}
	}
	frame.writeConsistency(batch.Cons)

	resp, err := c.call(frame)
	if err != nil {
		return err
	}
	switch x := resp.(type) {
	case resultVoidFrame:
	case error:
		return x
	default:
		return ErrProtocol
	}
	return nil
}

func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) Address() string {
	return c.addr
}

func (c *Conn) UseKeyspace(keyspace string) error {
	frame := make(frame, headerSize, defaultFrameSize)
	frame.setHeader(c.version, 0, 0, opQuery)
	frame.writeLongString("USE " + keyspace)
	frame.writeConsistency(1)
	frame.writeByte(0)

	resp, err := c.call(frame)
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

type queryInfo struct {
	id   []byte
	args []ColumnInfo
	rval []ColumnInfo
	wg   sync.WaitGroup
}

type callReq struct {
	active int32
	resp   chan callResp
}

type callResp struct {
	buf frame
	err error
}
