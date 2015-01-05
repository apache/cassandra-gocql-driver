// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Session is the interface used by users to interact with the database.
//
// It's safe for concurrent use by multiple goroutines and a typical usage
// scenario is to have one global session object to interact with the
// whole Cassandra cluster.
//
// This type extends the Node interface by adding a convinient query builder
// and automatically sets a default consinstency level on all operations
// that do not have a consistency level set.
type Session struct {
	Pool     ConnectionPool
	cons     Consistency
	pageSize int
	prefetch float64
	trace    Tracer
	mu       sync.RWMutex

	cfg ClusterConfig

	closeMu  sync.RWMutex
	isClosed bool
}

// NewSession wraps an existing Node.
func NewSession(p ConnectionPool, c ClusterConfig) *Session {
	return &Session{Pool: p, cons: Quorum, prefetch: 0.25, cfg: c}
}

// SetConsistency sets the default consistency level for this session. This
// setting can also be changed on a per-query basis and the default value
// is Quorum.
func (s *Session) SetConsistency(cons Consistency) {
	s.mu.Lock()
	s.cons = cons
	s.mu.Unlock()
}

// SetPageSize sets the default page size for this session. A value <= 0 will
// disable paging. This setting can also be changed on a per-query basis.
func (s *Session) SetPageSize(n int) {
	s.mu.Lock()
	s.pageSize = n
	s.mu.Unlock()
}

// SetPrefetch sets the default threshold for pre-fetching new pages. If
// there are only p*pageSize rows remaining, the next page will be requested
// automatically. This value can also be changed on a per-query basis and
// the default value is 0.25.
func (s *Session) SetPrefetch(p float64) {
	s.mu.Lock()
	s.prefetch = p
	s.mu.Unlock()
}

// SetTrace sets the default tracer for this session. This setting can also
// be changed on a per-query basis.
func (s *Session) SetTrace(trace Tracer) {
	s.mu.Lock()
	s.trace = trace
	s.mu.Unlock()
}

// Query generates a new query object for interacting with the database.
// Further details of the query may be tweaked using the resulting query
// value before the query is executed. Query is automatically prepared
// if it has not previously been executed.
func (s *Session) Query(stmt string, values ...interface{}) *Query {
	s.mu.RLock()
	qry := &Query{stmt: stmt, values: values, cons: s.cons,
		session: s, pageSize: s.pageSize, trace: s.trace,
		prefetch: s.prefetch, rt: s.cfg.RetryPolicy}
	s.mu.RUnlock()
	return qry
}

// Bind generates a new query object based on the query statement passed in.
// The query is automatically prepared if it has not previously been executed.
// The binding callback allows the application to define which query argument
// values will be marshalled as part of the query execution.
// During execution, the meta data of the prepared query will be routed to the
// binding callback, which is responsible for producing the query argument values.
func (s *Session) Bind(stmt string, b func(q *QueryInfo) ([]interface{}, error)) *Query {
	s.mu.RLock()
	qry := &Query{stmt: stmt, binding: b, cons: s.cons,
		session: s, pageSize: s.pageSize, trace: s.trace,
		prefetch: s.prefetch, rt: s.cfg.RetryPolicy}
	s.mu.RUnlock()
	return qry
}

// Close closes all connections. The session is unusable after this
// operation.
func (s *Session) Close() {

	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.isClosed {
		return
	}
	s.isClosed = true

	s.Pool.Close()
}

func (s *Session) Closed() bool {
	s.closeMu.RLock()
	closed := s.isClosed
	s.closeMu.RUnlock()
	return closed
}

func (s *Session) executeQuery(qry *Query) *Iter {

	// fail fast
	if s.Closed() {
		return &Iter{err: ErrSessionClosed}
	}

	var iter *Iter
	qry.attempts = 0
	qry.totalLatency = 0
	for {
		conn := s.Pool.Pick(qry)

		//Assign the error unavailable to the iterator
		if conn == nil {
			iter = &Iter{err: ErrNoConnections}
			break
		}

		t := time.Now()
		iter = conn.executeQuery(qry)
		qry.totalLatency += time.Now().Sub(t).Nanoseconds()
		qry.attempts++

		//Exit for loop if the query was successful
		if iter.err == nil {
			break
		}

		if qry.rt == nil || !qry.rt.Attempt(qry) {
			break
		}
	}

	return iter
}

// ExecuteBatch executes a batch operation and returns nil if successful
// otherwise an error is returned describing the failure.
func (s *Session) ExecuteBatch(batch *Batch) error {
	// fail fast
	if s.Closed() {
		return ErrSessionClosed
	}

	// Prevent the execution of the batch if greater than the limit
	// Currently batches have a limit of 65536 queries.
	// https://datastax-oss.atlassian.net/browse/JAVA-229
	if batch.Size() > BatchSizeMaximum {
		return ErrTooManyStmts
	}

	var err error
	batch.attempts = 0
	batch.totalLatency = 0
	for {
		conn := s.Pool.Pick(nil)

		//Assign the error unavailable and break loop
		if conn == nil {
			err = ErrNoConnections
			break
		}
		t := time.Now()
		err = conn.executeBatch(batch)
		batch.totalLatency += time.Now().Sub(t).Nanoseconds()
		batch.attempts++
		//Exit loop if operation executed correctly
		if err == nil {
			return nil
		}

		if batch.rt == nil || !batch.rt.Attempt(batch) {
			break
		}
	}

	return err
}

// Query represents a CQL statement that can be executed.
type Query struct {
	stmt         string
	values       []interface{}
	cons         Consistency
	pageSize     int
	pageState    []byte
	prefetch     float64
	trace        Tracer
	session      *Session
	rt           RetryPolicy
	binding      func(q *QueryInfo) ([]interface{}, error)
	attempts     int
	totalLatency int64
}

//Attempts returns the number of times the query was executed.
func (q *Query) Attempts() int {
	return q.attempts
}

//Latency returns the average amount of nanoseconds per attempt of the query.
func (q *Query) Latency() int64 {
	if q.attempts > 0 {
		return q.totalLatency / int64(q.attempts)
	}
	return 0
}

// Consistency sets the consistency level for this query. If no consistency
// level have been set, the default consistency level of the cluster
// is used.
func (q *Query) Consistency(c Consistency) *Query {
	q.cons = c
	return q
}

// GetConsistency returns the currently configured consistency level for
// the query.
func (q *Query) GetConsistency() Consistency {
	return q.cons
}

// Trace enables tracing of this query. Look at the documentation of the
// Tracer interface to learn more about tracing.
func (q *Query) Trace(trace Tracer) *Query {
	q.trace = trace
	return q
}

// PageSize will tell the iterator to fetch the result in pages of size n.
// This is useful for iterating over large result sets, but setting the
// page size to low might decrease the performance. This feature is only
// available in Cassandra 2 and onwards.
func (q *Query) PageSize(n int) *Query {
	q.pageSize = n
	return q
}

func (q *Query) shouldPrepare() bool {

	stmt := strings.TrimLeftFunc(strings.TrimRightFunc(q.stmt, func(r rune) bool {
		return unicode.IsSpace(r) || r == ';'
	}), unicode.IsSpace)

	var stmtType string
	if n := strings.IndexFunc(stmt, unicode.IsSpace); n >= 0 {
		stmtType = strings.ToLower(stmt[:n])
	}
	if stmtType == "begin" {
		if n := strings.LastIndexFunc(stmt, unicode.IsSpace); n >= 0 {
			stmtType = strings.ToLower(stmt[n+1:])
		}
	}
	switch stmtType {
	case "select", "insert", "update", "delete", "batch":
		return true
	}
	return false
}

// SetPrefetch sets the default threshold for pre-fetching new pages. If
// there are only p*pageSize rows remaining, the next page will be requested
// automatically.
func (q *Query) Prefetch(p float64) *Query {
	q.prefetch = p
	return q
}

// RetryPolicy sets the policy to use when retrying the query.
func (q *Query) RetryPolicy(r RetryPolicy) *Query {
	q.rt = r
	return q
}

// Bind sets query arguments of query. This can also be used to rebind new query arguments
// to an existing query instance.
func (q *Query) Bind(v ...interface{}) *Query {
	q.values = v
	return q
}

// Exec executes the query without returning any rows.
func (q *Query) Exec() error {
	iter := q.Iter()
	return iter.err
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (q *Query) Iter() *Iter {
	if strings.Index(strings.ToLower(q.stmt), "use") == 0 {
		return &Iter{err: ErrUseStmt}
	}
	return q.session.executeQuery(q)
}

// MapScan executes the query, copies the columns of the first selected
// row into the map pointed at by m and discards the rest. If no rows
// were selected, ErrNotFound is returned.
func (q *Query) MapScan(m map[string]interface{}) error {
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return err
	}
	iter.MapScan(m)
	return iter.Close()
}

// Scan executes the query, copies the columns of the first selected
// row into the values pointed at by dest and discards the rest. If no rows
// were selected, ErrNotFound is returned.
func (q *Query) Scan(dest ...interface{}) error {
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return err
	}
	iter.Scan(dest...)
	return iter.Close()
}

// ScanCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause). If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest.
func (q *Query) ScanCAS(dest ...interface{}) (applied bool, err error) {
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return false, err
	}
	if len(iter.Columns()) > 1 {
		dest = append([]interface{}{&applied}, dest...)
		iter.Scan(dest...)
	} else {
		iter.Scan(&applied)
	}
	return applied, iter.Close()
}

// MapScanCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause). If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest map.
//
// As for INSERT .. IF NOT EXISTS, previous values will be returned as if
// SELECT * FROM. So using ScanCAS with INSERT is inherently prone to
// column mismatching. MapScanCAS is added to capture them safely.
func (q *Query) MapScanCAS(dest map[string]interface{}) (applied bool, err error) {
	iter := q.Iter()
	if err := iter.checkErrAndNotFound(); err != nil {
		return false, err
	}
	iter.MapScan(dest)
	applied = dest["[applied]"].(bool)
	delete(dest, "[applied]")

	return applied, iter.Close()
}

// Iter represents an iterator that can be used to iterate over all rows that
// were returned by a query. The iterator might send additional queries to the
// database during the iteration if paging was enabled.
type Iter struct {
	err     error
	pos     int
	rows    [][][]byte
	columns []ColumnInfo
	next    *nextIter
}

// Columns returns the name and type of the selected columns.
func (iter *Iter) Columns() []ColumnInfo {
	return iter.columns
}

// Scan consumes the next row of the iterator and copies the columns of the
// current row into the values pointed at by dest. Use nil as a dest value
// to skip the corresponding column. Scan might send additional queries
// to the database to retrieve the next set of rows if paging was enabled.
//
// Scan returns true if the row was successfully unmarshaled or false if the
// end of the result set was reached or if an error occurred. Close should
// be called afterwards to retrieve any potential errors.
func (iter *Iter) Scan(dest ...interface{}) bool {
	if iter.err != nil {
		return false
	}
	if iter.pos >= len(iter.rows) {
		if iter.next != nil {
			*iter = *iter.next.fetch()
			return iter.Scan(dest...)
		}
		return false
	}
	if iter.next != nil && iter.pos == iter.next.pos {
		go iter.next.fetch()
	}
	if len(dest) != len(iter.columns) {
		iter.err = errors.New("count mismatch")
		return false
	}
	for i := 0; i < len(iter.columns); i++ {
		if dest[i] == nil {
			continue
		}
		err := Unmarshal(iter.columns[i].TypeInfo, iter.rows[iter.pos][i], dest[i])
		if err != nil {
			iter.err = err
			return false
		}
	}
	iter.pos++
	return true
}

// Close closes the iterator and returns any errors that happened during
// the query or the iteration.
func (iter *Iter) Close() error {
	return iter.err
}

// checkErrAndNotFound handle error and NotFound in one method.
func (iter *Iter) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
	} else if len(iter.rows) == 0 {
		return ErrNotFound
	}
	return nil
}

type nextIter struct {
	qry  Query
	pos  int
	once sync.Once
	next *Iter
}

func (n *nextIter) fetch() *Iter {
	n.once.Do(func() {
		n.next = n.qry.session.executeQuery(&n.qry)
	})
	return n.next
}

type Batch struct {
	Type         BatchType
	Entries      []BatchEntry
	Cons         Consistency
	rt           RetryPolicy
	attempts     int
	totalLatency int64
}

// NewBatch creates a new batch operation without defaults from the cluster
func NewBatch(typ BatchType) *Batch {
	return &Batch{Type: typ}
}

// NewBatch creates a new batch operation using defaults defined in the cluster
func (s *Session) NewBatch(typ BatchType) *Batch {
	return &Batch{Type: typ, rt: s.cfg.RetryPolicy}
}

// Attempts returns the number of attempts made to execute the batch.
func (b *Batch) Attempts() int {
	return b.attempts
}

//Latency returns the average number of nanoseconds to execute a single attempt of the batch.
func (b *Batch) Latency() int64 {
	if b.attempts > 0 {
		return b.totalLatency / int64(b.attempts)
	}
	return 0
}

// GetConsistency returns the currently configured consistency level for the batch
// operation.
func (b *Batch) GetConsistency() Consistency {
	return b.Cons
}

// Query adds the query to the batch operation
func (b *Batch) Query(stmt string, args ...interface{}) {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, Args: args})
}

// Bind adds the query to the batch operation and correlates it with a binding callback
// that will be invoked when the batch is executed. The binding callback allows the application
// to define which query argument values will be marshalled as part of the batch execution.
func (b *Batch) Bind(stmt string, bind func(q *QueryInfo) ([]interface{}, error)) {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, binding: bind})
}

// RetryPolicy sets the retry policy to use when executing the batch operation
func (b *Batch) RetryPolicy(r RetryPolicy) *Batch {
	b.rt = r
	return b
}

// Size returns the number of batch statements to be executed by the batch operation.
func (b *Batch) Size() int {
	return len(b.Entries)
}

type BatchType int

const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

type BatchEntry struct {
	Stmt    string
	Args    []interface{}
	binding func(q *QueryInfo) ([]interface{}, error)
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
	LocalOne
)

var ConsistencyNames = []string{
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
	LocalOne:    "localone",
}

func (c Consistency) String() string {
	return ConsistencyNames[c]
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo *TypeInfo
}

// Tracer is the interface implemented by query tracers. Tracers have the
// ability to obtain a detailed event log of all events that happened during
// the execution of a query from Cassandra. Gathering this information might
// be essential for debugging and optimizing queries, but this feature should
// not be used on production systems with very high load.
type Tracer interface {
	Trace(traceId []byte)
}

type traceWriter struct {
	session *Session
	w       io.Writer
	mu      sync.Mutex
}

// NewTraceWriter returns a simple Tracer implementation that outputs
// the event log in a textual format.
func NewTraceWriter(session *Session, w io.Writer) Tracer {
	return &traceWriter{session: session, w: w}
}

func (t *traceWriter) Trace(traceId []byte) {
	var (
		coordinator string
		duration    int
	)
	t.session.Query(`SELECT coordinator, duration
			FROM system_traces.sessions
			WHERE session_id = ?`, traceId).
		Consistency(One).Scan(&coordinator, &duration)

	iter := t.session.Query(`SELECT event_id, activity, source, source_elapsed
			FROM system_traces.events
			WHERE session_id = ?`, traceId).
		Consistency(One).Iter()
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
	ErrNotFound      = errors.New("not found")
	ErrUnavailable   = errors.New("unavailable")
	ErrUnsupported   = errors.New("feature not supported")
	ErrTooManyStmts  = errors.New("too many statements")
	ErrUseStmt       = errors.New("use statements aren't supported. Please see https://github.com/gocql/gocql for explaination.")
	ErrSessionClosed = errors.New("session has been closed")
	ErrNoConnections = errors.New("no connections available")
)

type ErrProtocol struct{ error }

func NewErrProtocol(format string, args ...interface{}) error {
	return ErrProtocol{fmt.Errorf(format, args...)}
}

// BatchSizeMaximum is the maximum number of statements a batch operation can have.
// This limit is set by cassandra and could change in the future.
const BatchSizeMaximum = 65535
