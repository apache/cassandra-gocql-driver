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

type connection struct {
	conn     net.Conn
	uniq     chan uint8
	requests []frameRequest
	nwait    int32

	prepMu sync.Mutex
	prep   map[string]*queryInfo

	timeout time.Duration
}

func connect(addr string, cfg *Config) (*connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &connection{
		conn:     conn,
		uniq:     make(chan uint8, 64),
		requests: make([]frameRequest, 64),
		prep:     make(map[string]*queryInfo),
		timeout:  cfg.Timeout,
	}
	for i := 0; i < cap(c.uniq); i++ {
		c.uniq <- uint8(i)
	}

	go c.run()

	frame := make(buffer, headerSize)
	frame.setHeader(protoRequest, 0, 0, opStartup)
	frame.writeStringMap(map[string]string{
		"CQL_VERSION": cfg.CQLVersion,
	})
	frame.setLength(len(frame) - headerSize)

	frame, err = c.request(frame)
	if err != nil {
		return nil, err
	}

	if cfg.Keyspace != "" {
		qry := &Query{stmt: "USE " + cfg.Keyspace}
		frame, err = c.executeQuery(qry)
	}

	return c, nil
}

func (c *connection) run() {
	var err error
	for {
		var frame buffer
		frame, err = c.recv()
		if err != nil {
			break
		}
		c.dispatch(frame)
	}

	c.conn.Close()
	for id := 0; id < len(c.requests); id++ {
		req := &c.requests[id]
		if atomic.LoadInt32(&req.active) == 1 {
			req.reply <- frameReply{nil, err}
		}
	}
}

func (c *connection) recv() (buffer, error) {
	frame := make(buffer, headerSize, headerSize+512)
	c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	n, last, pinged := 0, 0, false
	for n < len(frame) {
		nn, err := c.conn.Read(frame[n:])
		n += nn
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
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
		if n == headerSize && len(frame) == headerSize {
			if frame[0] != protoResponse {
				return nil, ErrInvalid
			}
			frame.grow(frame.Length())
		}
	}
	return frame, nil
}

func (c *connection) ping() error {
	frame := make(buffer, headerSize, headerSize)
	frame.setHeader(protoRequest, 0, 0, opOptions)
	frame.setLength(0)

	_, err := c.request(frame)
	return err
}

func (c *connection) request(frame buffer) (buffer, error) {
	id := <-c.uniq
	frame[2] = id

	req := &c.requests[id]
	req.reply = make(chan frameReply, 1)
	atomic.AddInt32(&c.nwait, 1)
	atomic.StoreInt32(&req.active, 1)

	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	reply := <-req.reply
	req.reply = nil

	c.uniq <- id
	return reply.buf, reply.err
}

func (c *connection) dispatch(frame buffer) {
	id := int(frame[2])
	if id >= len(c.requests) {
		return
	}
	req := &c.requests[id]
	if !atomic.CompareAndSwapInt32(&req.active, 1, 0) {
		return
	}
	atomic.AddInt32(&c.nwait, -1)
	req.reply <- frameReply{frame, nil}
}

func (c *connection) prepareQuery(stmt string) *queryInfo {
	c.prepMu.Lock()
	info := c.prep[stmt]
	if info != nil {
		c.prepMu.Unlock()
		info.wg.Wait()
		return info
	}
	info = new(queryInfo)
	info.wg.Add(1)
	c.prep[stmt] = info
	c.prepMu.Unlock()

	frame := make(buffer, headerSize, headerSize+512)
	frame.setHeader(protoRequest, 0, 0, opPrepare)
	frame.writeLongString(stmt)
	frame.setLength(len(frame) - headerSize)

	frame, err := c.request(frame)
	if err != nil {
		return nil
	}
	frame.skipHeader()
	frame.readInt() // kind
	info.id = frame.readShortBytes()
	info.args = frame.readMetaData()
	info.rval = frame.readMetaData()
	info.wg.Done()
	return info
}

func (c *connection) executeQuery(query *Query) (buffer, error) {
	var info *queryInfo
	if len(query.args) > 0 {
		info = c.prepareQuery(query.stmt)
	}

	frame := make(buffer, headerSize, headerSize+512)
	if info == nil {
		frame.setHeader(protoRequest, 0, 0, opQuery)
		frame.writeLongString(query.stmt)
	} else {
		frame.setHeader(protoRequest, 0, 0, opExecute)
		frame.writeShortBytes(info.id)
	}
	frame.writeShort(uint16(query.cons))
	flags := uint8(0)
	if len(query.args) > 0 {
		flags |= flagQueryValues
	}
	frame.writeByte(flags)
	if len(query.args) > 0 {
		frame.writeShort(uint16(len(query.args)))
		for i := 0; i < len(query.args); i++ {
			val, err := Marshal(info.args[i].TypeInfo, query.args[i])
			if err != nil {
				return nil, err
			}
			frame.writeBytes(val)
		}
	}
	frame.setLength(len(frame) - headerSize)

	frame, err := c.request(frame)
	if err != nil {
		return nil, err
	}

	if frame[3] == opError {
		frame.skipHeader()
		code := frame.readInt()
		desc := frame.readString()
		return nil, Error{code, desc}
	}
	return frame, nil
}

type queryInfo struct {
	id   []byte
	args []columnInfo
	rval []columnInfo
	wg   sync.WaitGroup
}

type frameRequest struct {
	active int32
	reply  chan frameReply
}

type frameReply struct {
	buf buffer
	err error
}
