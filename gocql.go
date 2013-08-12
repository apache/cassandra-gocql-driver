// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Nodes       []string
	CQLVersion  string
	Keyspace    string
	Consistency Consistency
	DefaultPort int
	Timeout     time.Duration
	NodePicker  NodePicker
	Reconnector Reconnector
}

func (c *Config) normalize() {
	if c.CQLVersion == "" {
		c.CQLVersion = "3.0.0"
	}
	if c.DefaultPort == 0 {
		c.DefaultPort = 9042
	}
	if c.Timeout <= 0 {
		c.Timeout = 200 * time.Millisecond
	}
	if c.NodePicker == nil {
		c.NodePicker = NewRoundRobinPicker()
	}
	if c.Reconnector == nil {
		c.Reconnector = NewExponentialReconnector(1*time.Second, 10*time.Minute)
	}
	for i := 0; i < len(c.Nodes); i++ {
		c.Nodes[i] = strings.TrimSpace(c.Nodes[i])
		if strings.IndexByte(c.Nodes[i], ':') < 0 {
			c.Nodes[i] = fmt.Sprintf("%s:%d", c.Nodes[i], c.DefaultPort)
		}
	}
}

type Session struct {
	cfg         *Config
	pool        NodePicker
	reconnector Reconnector
	keyspace    string
	nohosts     chan bool
}

func NewSession(cfg Config) *Session {
	cfg.normalize()
	s := &Session{
		cfg:         &cfg,
		nohosts:     make(chan bool),
		reconnector: cfg.Reconnector,
		pool:        cfg.NodePicker,
	}
	for _, address := range cfg.Nodes {
		go s.reconnector.Reconnect(s, address)
	}
	return s
}

func (s *Session) Query(stmt string, args ...interface{}) *Query {
	return &Query{
		stmt: stmt,
		args: args,
		cons: s.cfg.Consistency,
		ctx:  s,
	}
}

func (s *Session) Close() {
	return
}

func (s *Session) executeQuery(query *Query) (frame, error) {
	node := s.pool.Pick(query)
	if node == nil {
		<-time.After(s.cfg.Timeout)
		node = s.pool.Pick(query)
	}
	if node == nil {
		return nil, ErrNoHostAvailable
	}
	return node.conn.executeQuery(query)
}

type Node struct {
	conn *Conn
}

type Query struct {
	stmt string
	args []interface{}
	cons Consistency
	ctx  queryContext
}

func NewQuery(stmt string) *Query {
	return &Query{stmt: stmt, cons: ConQuorum}
}

func (q *Query) Exec() error {
	frame, err := q.request()
	if err != nil {
		return err
	}
	if frame[3] == opResult {
		frame.skipHeader()
		kind := frame.readInt()
		if kind == 3 {
			keyspace := frame.readString()
			fmt.Println("set keyspace:", keyspace)
		} else {
		}
	}
	return nil
}

func (q *Query) Iter() *Iter {
	iter := new(Iter)
	frame, err := q.request()
	if err != nil {
		iter.err = err
		return iter
	}
	frame.skipHeader()
	kind := frame.readInt()
	if kind == resultKindRows {
		iter.setFrame(frame)
	}
	return iter
}

func (q *Query) Scan(values ...interface{}) error {
	found := false
	iter := q.Iter()
	if iter.Scan(values...) {
		found = true
	}
	if err := iter.Close(); err != nil {
		return err
	} else if !found {
		return ErrNotFound
	}
	return nil
}

func (q *Query) Consistency(cons Consistency) *Query {
	q.cons = cons
	return q
}

func (q *Query) request() (frame, error) {
	return q.ctx.executeQuery(q)
}

type Iter struct {
	err     error
	pos     int
	numRows int
	info    []columnInfo
	flags   int
	frame   frame
}

func (iter *Iter) setFrame(frame frame) {
	info := frame.readMetaData()
	iter.flags = 0
	iter.info = info
	iter.numRows = frame.readInt()
	iter.pos = 0
	iter.err = nil
	iter.frame = frame
}

func (iter *Iter) Scan(values ...interface{}) bool {
	if iter.err != nil || iter.pos >= iter.numRows {
		return false
	}
	iter.pos++
	if len(values) != len(iter.info) {
		iter.err = errors.New("count mismatch")
		return false
	}
	for i := 0; i < len(values); i++ {
		data := iter.frame.readBytes()
		if err := Unmarshal(iter.info[i].TypeInfo, data, values[i]); err != nil {
			iter.err = err
			return false
		}
	}
	return true
}

func (iter *Iter) Close() error {
	return iter.err
}

type queryContext interface {
	executeQuery(query *Query) (frame, error)
}

type columnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo *TypeInfo
}

type Consistency uint16

const (
	ConAny         Consistency = 0x0000
	ConOne         Consistency = 0x0001
	ConTwo         Consistency = 0x0002
	ConThree       Consistency = 0x0003
	ConQuorum      Consistency = 0x0004
	ConAll         Consistency = 0x0005
	ConLocalQuorum Consistency = 0x0006
	ConEachQuorum  Consistency = 0x0007
	ConSerial      Consistency = 0x0008
	ConLocalSerial Consistency = 0x0009
)

type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

var (
	ErrNotFound        = errors.New("not found")
	ErrNoHostAvailable = errors.New("no host available")
	ErrQueryUnbound    = errors.New("can not execute unbound query")
	ErrProtocol        = errors.New("protocol error")
)

type node struct {
	conn *Conn
}
