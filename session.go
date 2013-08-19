// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Session is the interface used by users to interact with the database.
//
// It extends the Node interface by adding a convinient query builder and
// automatically sets a default consinstency level on all operations
// that do not have a consistency level set.
type Session struct {
	Node Node
	Cons Consistency
}

// NewSession wraps an existing Node.
func NewSession(node Node) *Session {
	return &Session{Node: node, Cons: Quorum}
}

// Query can be used to build new queries that should be executed on this
// session.
func (s *Session) Query(stmt string, args ...interface{}) QueryBuilder {
	return QueryBuilder{NewQuery(stmt, args...), s}
}

// Do can be used to modify a copy of an existing query before it is
// executed on this session.
func (s *Session) Do(qry *Query) QueryBuilder {
	q := *qry
	return QueryBuilder{&q, s}
}

// Close closes all connections. The session is unuseable after this
// operation.
func (s *Session) Close() {
	s.Node.Close()
}

// ExecuteQuery executes a Query on the underlying Node.
func (s *Session) ExecuteQuery(qry *Query) *Iter {
	return s.executeQuery(qry, nil)
}

func (s *Session) executeQuery(qry *Query, pageState []byte) *Iter {
	if qry.Cons == 0 {
		qry.Cons = s.Cons
	}
	conn := s.Node.Pick(qry)
	if conn == nil {
		return &Iter{err: ErrUnavailable}
	}
	iter := conn.executeQuery(qry, pageState)
	if len(iter.pageState) > 0 {
		iter.qry = qry
		iter.session = s
	}
	return iter
}

func (s *Session) ExecuteBatch(batch *Batch) error {
	conn := s.Node.Pick(nil)
	if conn == nil {
		return ErrUnavailable
	}
	return conn.executeBatch(batch)
}

type Query struct {
	Stmt     string
	Args     []interface{}
	Cons     Consistency
	Token    string
	PageSize int
	Trace    Tracer
}

func NewQuery(stmt string, args ...interface{}) *Query {
	return &Query{Stmt: stmt, Args: args}
}

type QueryBuilder struct {
	qry     *Query
	session *Session
}

// Args specifies the query parameters.
func (b QueryBuilder) Args(args ...interface{}) {
	b.qry.Args = args
}

// Consistency sets the consistency level for this query. If no consistency
// level have been set, the default consistency level of the cluster
// is used.
func (b QueryBuilder) Consistency(cons Consistency) QueryBuilder {
	b.qry.Cons = cons
	return b
}

func (b QueryBuilder) Token(token string) QueryBuilder {
	b.qry.Token = token
	return b
}

// Trace enables tracing of this query. Look at the documentation of the
// Tracer interface to learn more about tracing.
func (b QueryBuilder) Trace(trace Tracer) QueryBuilder {
	b.qry.Trace = trace
	return b
}

func (b QueryBuilder) PageSize(size int) QueryBuilder {
	b.qry.PageSize = size
	return b
}

func (b QueryBuilder) Exec() error {
	iter := b.session.ExecuteQuery(b.qry)
	return iter.err
}

func (b QueryBuilder) Iter() *Iter {
	return b.session.ExecuteQuery(b.qry)
}

func (b QueryBuilder) Scan(values ...interface{}) error {
	iter := b.Iter()
	iter.Scan(values...)
	return iter.Close()
}

type Iter struct {
	err       error
	pos       int
	rows      [][][]byte
	columns   []ColumnInfo
	qry       *Query
	session   *Session
	pageState []byte
}

func (iter *Iter) Columns() []ColumnInfo {
	return iter.columns
}

func (iter *Iter) Scan(values ...interface{}) bool {
	if iter.err != nil {
		return false
	}
	if iter.pos >= len(iter.rows) {
		if len(iter.pageState) > 0 {
			*iter = *iter.session.executeQuery(iter.qry, iter.pageState)
			return iter.Scan(values...)
		}
		return false
	}
	if len(values) != len(iter.columns) {
		iter.err = errors.New("count mismatch")
		return false
	}
	for i := 0; i < len(iter.columns); i++ {
		err := Unmarshal(iter.columns[i].TypeInfo, iter.rows[iter.pos][i], values[i])
		if err != nil {
			iter.err = err
			return false
		}
	}
	iter.pos++
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

func NewBatch(typ BatchType) *Batch {
	return &Batch{Type: typ}
}

func (b *Batch) Query(stmt string, args ...interface{}) {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, Args: args})
}

type BatchType int

const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

type BatchEntry struct {
	Stmt string
	Args []interface{}
}

type Consistency int

const (
	Any Consistency = 1 + iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	Serial
	LocalSerial
)

var consinstencyNames = []string{
	0:           "default",
	Any:         "any",
	One:         "one",
	Two:         "two",
	Three:       "three",
	Quorum:      "quorum",
	All:         "all",
	LocalQuorum: "localquorum",
	EachQuorum:  "eachquorum",
	Serial:      "serial",
	LocalSerial: "localserial",
}

func (c Consistency) String() string {
	return consinstencyNames[c]
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo *TypeInfo
}

// Tracer is the interface implemented by query tracers. Tracers have the
// ability to obtain a detailed event log of all events that happend during
// the execution of a query from Cassandra. Gathering this information might
// be essential for debugging and optimizing queries, but this feature should
// not be used on production systems with very high load.
type Tracer interface {
	Trace(conn *Conn, traceId []byte)
}

type traceWriter struct {
	w  io.Writer
	mu sync.Mutex
}

// NewTraceWriter returns a simple Tracer implementation that outputs
// the event log in a textual format.
func NewTraceWriter(w io.Writer) Tracer {
	return traceWriter{w: w}
}

func (t traceWriter) Trace(conn *Conn, traceId []byte) {
	var (
		coordinator string
		duration    int
	)
	conn.executeQuery(&Query{
		Stmt: `SELECT coordinator, duration
			FROM system_traces.sessions
			WHERE session_id = ?`,
		Args: []interface{}{traceId},
		Cons: One,
	}, nil).Scan(&coordinator, &duration)

	iter := conn.executeQuery(&Query{
		Stmt: `SELECT event_id, activity, source, source_elapsed
			FROM system_traces.events
			WHERE session_id = ?`,
		Args: []interface{}{traceId},
		Cons: One,
	}, nil)
	var (
		timestamp time.Time
		activity  string
		source    string
		elapsed   int
	)
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Fprintf(t.w, "Tracing session %016x (coordinator: %s, duration: %v):\n",
		traceId, coordinator, time.Duration(duration)*time.Microsecond)
	for iter.Scan(&timestamp, &activity, &source, &elapsed) {
		fmt.Fprintf(t.w, "%s: %s (source: %s, elapsed: %d)\n",
			timestamp.Format("2006/01/02 15:04:05.999999"), activity, source, elapsed)
	}
	if err := iter.Close(); err != nil {
		fmt.Fprintln(t.w, "Error:", err)
	}
}

type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

var (
	ErrNotFound    = errors.New("not found")
	ErrUnavailable = errors.New("unavailable")
	ErrProtocol    = errors.New("protocol error")
	ErrUnsupported = errors.New("feature not supported")
)
