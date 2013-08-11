package gocql

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type queryInfo struct {
	id    []byte
	args  []columnInfo
	rval  []columnInfo
	avail chan bool
}

type connection struct {
	conn    net.Conn
	uniq    chan uint8
	reply   []chan buffer
	waiting uint64

	prepMu sync.Mutex
	prep   map[string]*queryInfo
}

func connect(addr string, cfg *Config) (*connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &connection{
		conn:  conn,
		uniq:  make(chan uint8, 64),
		reply: make([]chan buffer, 64),
		prep:  make(map[string]*queryInfo),
	}
	for i := 0; i < cap(c.uniq); i++ {
		c.uniq <- uint8(i)
	}

	go c.recv()

	frame := make(buffer, headerSize)
	frame.setHeader(protoRequest, 0, 0, opStartup)
	frame.writeStringMap(map[string]string{
		"CQL_VERSION": cfg.CQLVersion,
	})
	frame.setLength(len(frame) - headerSize)

	frame = c.request(frame)

	if cfg.Keyspace != "" {
		qry := &Query{stmt: "USE " + cfg.Keyspace}
		frame, err = c.executeQuery(qry)
	}

	return c, nil
}

func (c *connection) recv() {
	for {
		frame := make(buffer, headerSize, headerSize+512)
		if _, err := io.ReadFull(c.conn, frame); err != nil {
			return
		}
		if frame[0] != protoResponse {
			continue
		}
		if length := frame.Length(); length > 0 {
			frame.grow(frame.Length())
			io.ReadFull(c.conn, frame[headerSize:])
		}
		c.dispatch(frame)
	}
	panic("not possible")
}

func (c *connection) request(frame buffer) buffer {
	id := <-c.uniq
	frame[2] = id
	c.reply[id] = make(chan buffer, 1)

	for {
		w := atomic.LoadUint64(&c.waiting)
		if atomic.CompareAndSwapUint64(&c.waiting, w, w|(1<<id)) {
			break
		}
	}
	c.conn.Write(frame)
	resp := <-c.reply[id]
	c.uniq <- id
	return resp
}

func (c *connection) dispatch(frame buffer) {
	id := frame[2]
	if id >= 128 {
		return
	}
	for {
		w := atomic.LoadUint64(&c.waiting)
		if w&(1<<id) == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&c.waiting, w, w&^(1<<id)) {
			break
		}
	}
	c.reply[id] <- frame
}

func (c *connection) prepareQuery(stmt string) *queryInfo {
	c.prepMu.Lock()
	info := c.prep[stmt]
	if info != nil {
		c.prepMu.Unlock()
		<-info.avail
		return info
	}
	info = &queryInfo{avail: make(chan bool)}
	c.prep[stmt] = info
	c.prepMu.Unlock()

	frame := make(buffer, headerSize, headerSize+512)
	frame.setHeader(protoRequest, 0, 0, opPrepare)
	frame.writeLongString(stmt)
	frame.setLength(len(frame) - headerSize)

	frame = c.request(frame)
	frame.skipHeader()
	frame.readInt() // kind
	info.id = frame.readShortBytes()
	info.args = frame.readMetaData()
	info.rval = frame.readMetaData()
	close(info.avail)
	return info
}

func (c *connection) executeQuery(query *Query) (buffer, error) {
	var info *queryInfo
	if len(query.args) > 0 {
		info = c.prepareQuery(query.stmt)
	}

	frame := make(buffer, headerSize, headerSize+512)
	frame.setHeader(protoRequest, 0, 0, opQuery)
	frame.writeLongString(query.stmt)
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

	frame = c.request(frame)

	if frame[3] == opError {
		frame.skipHeader()
		code := frame.readInt()
		desc := frame.readString()
		return nil, Error{code, desc}
	}
	return frame, nil
}
