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

func (s *Session) Query(stmt string, args ...interface{}) QueryBuilder {
	return QueryBuilder{
		&Query{
			Stmt: stmt,
			Args: args,
			Cons: s.cfg.Consistency,
		},
		s,
	}
}

func (s *Session) Do(qry *Query) QueryBuilder {
	q := *qry
	return QueryBuilder{&q, s}
}

func (s *Session) Close() {
	return
}

func (s *Session) ExecuteBatch(batch *Batch) error {
	return nil
}

func (s *Session) ExecuteQuery(qry *Query) (*Iter, error) {
	node := s.pool.Pick(qry)
	if node == nil {
		<-time.After(s.cfg.Timeout)
		node = s.pool.Pick(qry)
	}
	if node == nil {
		return nil, ErrNoHostAvailable
	}
	return node.ExecuteQuery(qry)
}

type Query struct {
	Stmt     string
	Args     []interface{}
	Cons     Consistency
	PageSize int
	Trace    bool
}

type QueryBuilder struct {
	qry *Query
	ctx Node
}

func (b QueryBuilder) Args(args ...interface{}) {
	b.qry.Args = args
}

func (b QueryBuilder) Consistency(cons Consistency) QueryBuilder {
	b.qry.Cons = cons
	return b
}

func (b QueryBuilder) Trace(trace bool) QueryBuilder {
	b.qry.Trace = trace
	return b
}

func (b QueryBuilder) PageSize(size int) QueryBuilder {
	b.qry.PageSize = size
	return b
}

func (b QueryBuilder) Exec() error {
	_, err := b.ctx.ExecuteQuery(b.qry)
	return err
}

func (b QueryBuilder) Iter() *Iter {
	iter, err := b.ctx.ExecuteQuery(b.qry)
	if err != nil {
		return &Iter{err: err}
	}
	return iter
}

func (b QueryBuilder) Scan(values ...interface{}) error {
	iter := b.Iter()
	iter.Scan(values...)
	return iter.Close()
}

type Iter struct {
	err    error
	pos    int
	values [][]byte
	info   []ColumnInfo
}

func (iter *Iter) readFrame(frame frame) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && e == ErrProtocol {
				iter.err = e
				return
			}
			panic(r)
		}
	}()
	frame.skipHeader()
	iter.pos = 0
	iter.err = nil
	iter.values = nil
	if frame.readInt() != resultKindRows {
		return
	}
	iter.info = frame.readMetaData()
	numRows := frame.readInt()
	iter.values = make([][]byte, numRows*len(iter.info))
	for i := 0; i < len(iter.values); i++ {
		iter.values[i] = frame.readBytes()
	}
}

func (iter *Iter) Columns() []ColumnInfo {
	return iter.info
}

func (iter *Iter) Scan(values ...interface{}) bool {
	if iter.err != nil || iter.pos >= len(iter.values) {
		return false
	}
	if len(values) != len(iter.info) {
		iter.err = errors.New("count mismatch")
		return false
	}
	for i := 0; i < len(values); i++ {
		err := Unmarshal(iter.info[i].TypeInfo, iter.values[i+iter.pos], values[i])
		if err != nil {
			iter.err = err
			return false
		}
	}
	iter.pos += len(values)
	return true
}

func (iter *Iter) Close() error {
	return iter.err
}

type Batch struct {
	Type    BatchType
	Entries []BatchEntry
	Cons    Consistency
}

func NewBatch(typ BatchType, cons Consistency) *Batch {
	return &Batch{Type: typ, Cons: cons}
}

func (b *Batch) Query(stmt string, args ...interface{}) {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, Args: args})
}

type BatchEntry struct {
	Stmt string
	Args []interface{}
}
