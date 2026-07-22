//go:build all || unit
// +build all unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"
	"github.com/stretchr/testify/require"
)

type unitFuncBatchObserver func(context.Context, ObservedBatch)

func (f unitFuncBatchObserver) ObserveBatch(ctx context.Context, o ObservedBatch) { f(ctx, o) }

func TestObserveByteMetricsPrepareAndRecovery(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, recoverUnprepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%t/reprepare=%t", batch, recoverUnprepared), func(t *testing.T) {
				var mu sync.Mutex
				var expected byteCounts
				var requests int
				var rejected bool
				srv := newTestServerOpts{
					addr: "127.0.0.1:0", protocol: protoVersion4,
					customRequestHandler: func(_ *TestServer, req, resp *framer) error {
						op := req.header.op
						switch op {
						case opStartup:
							resp.writeHeader(0, opReady, req.header.stream)
						case opOptions:
							resp.writeHeader(0, opSupported, req.header.stream)
							resp.writeShort(0)
						case opPrepare:
							resp.writeHeader(0, opResult, req.header.stream)
							resp.writeInt(resultKindPrepared)
							resp.writeShortBytes([]byte{1})
							resp.writeInt(0) // prepared metadata flags
							resp.writeInt(0) // bound columns
							resp.writeInt(0) // partition key columns
							resp.writeInt(0) // result metadata flags
							resp.writeInt(0) // result columns
						case opExecute, opBatch:
							if recoverUnprepared && !rejected {
								rejected = true
								resp.writeHeader(0, opError, req.header.stream)
								resp.writeInt(ErrCodeUnprepared)
								resp.writeString("unprepared")
								resp.writeShortBytes([]byte{1})
							} else {
								resp.writeHeader(0, opResult, req.header.stream)
								resp.writeInt(resultKindVoid)
							}
						default:
							return fmt.Errorf("unexpected opcode %v", op)
						}
						if op == opPrepare || op == opExecute || op == opBatch {
							mu.Lock()
							requests++
							expected.bytesTx += int64(req.header.length)
							expected.bytesRx += int64(len(resp.buf) - frameHeadSize)
							mu.Unlock()
						}
						return nil
					},
				}.newServer(t, context.Background())
				defer srv.Stop()
				cluster := testCluster(protoVersion4, srv.Address)
				cluster.NumConns = 1
				db, err := cluster.CreateSession()
				require.NoError(t, err)
				defer db.Close()

				var observed byteCounts
				var events int
				var execute func() error
				if batch {
					b := db.NewBatch(LoggedBatch)
					b.Bind("select value from table", func(*QueryInfo) ([]interface{}, error) { return nil, nil })
					b.Observer(unitFuncBatchObserver(func(_ context.Context, o ObservedBatch) {
						events++
						observed = byteCounts{o.BytesTx, o.BytesRx}
					}))
					execute = b.Exec
				} else {
					q := db.Query("select value from table").RoutingKey([]byte{1})
					q.Observer(unitFuncQueryObserver(func(_ context.Context, o ObservedQuery) {
						events++
						observed = byteCounts{o.BytesTx, o.BytesRx}
					}))
					execute = q.Exec
				}

				// One observation must include PREPARE and all UNPREPARED recovery
				// exchanges, even though only the final response reaches the iterator.
				require.NoError(t, execute())
				require.Equal(t, 1, events)
				mu.Lock()
				want := expected
				requestCount := requests
				expected = byteCounts{}
				requests = 0
				mu.Unlock()
				require.Equal(t, want, observed)
				wantRequests := 2
				if recoverUnprepared {
					wantRequests = 4
				}
				require.Equal(t, wantRequests, requestCount)

				// A separate execution hits the prepared cache and starts fresh.
				require.NoError(t, execute())
				require.Equal(t, 2, events)
				mu.Lock()
				want = expected
				requestCount = requests
				mu.Unlock()
				require.Equal(t, want, observed)
				require.Equal(t, 1, requestCount)
			})
		}
	}
}

type sameHostByteMetricsRetryPolicy struct{ SimpleRetryPolicy }

func (*sameHostByteMetricsRetryPolicy) GetRetryType(error) RetryType { return Retry }

func TestObserveByteMetricsRetries(t *testing.T) {
	srv := NewTestServer(t, protoVersion4, context.Background())
	defer srv.Stop()
	db, err := newTestSession(protoVersion4, srv.Address)
	require.NoError(t, err)
	defer db.Close()
	var events []ObservedQuery
	observer := unitFuncQueryObserver(func(_ context.Context, o ObservedQuery) { events = append(events, o) })
	err = db.Query("kill").Idempotent(true).RetryPolicy(&sameHostByteMetricsRetryPolicy{SimpleRetryPolicy{NumRetries: 2}}).Observer(observer).Exec()
	require.Error(t, err)
	require.Len(t, events, 3)
	for i, o := range events {
		require.Equal(t, i, o.Attempt)
		require.Equal(t, events[0].BytesTx, o.BytesTx)
		require.Equal(t, events[0].BytesRx, o.BytesRx)
		require.Positive(t, o.BytesTx)
		require.Equal(t, int64(4+2+len("query killed")), o.BytesRx) // error code and message
	}
}

type repeatingByteMetricsHostPolicy struct{ HostSelectionPolicy }

func (p repeatingByteMetricsHostPolicy) Pick(q ExecutableStatement) NextHost {
	host := p.HostSelectionPolicy.Pick(q)()
	return func() SelectedHost { return host }
}

func TestObserveByteMetricsSpeculation(t *testing.T) {
	srv := NewTestServer(t, protoVersion4, context.Background())
	defer srv.Stop()
	cluster := testCluster(protoVersion4, srv.Address)
	// Allow two speculative attempts on one loopback host, using the real
	// executor and connection pool without requiring extra loopback aliases.
	cluster.PoolConfig.HostSelectionPolicy = repeatingByteMetricsHostPolicy{RoundRobinHostPolicy()}
	db, err := cluster.CreateSession()
	require.NoError(t, err)
	defer db.Close()
	events := make(chan ObservedQuery, 2)
	observer := unitFuncQueryObserver(func(_ context.Context, o ObservedQuery) { events <- o })
	q := db.Query("slow").Idempotent(true).Observer(observer).SetSpeculativeExecutionPolicy(
		&SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: time.Millisecond},
	)
	require.NoError(t, q.Exec())
	var success, canceled int
	for i := 0; i < 2; i++ {
		select {
		case o := <-events:
			require.Positive(t, o.BytesTx)
			if o.Err == nil {
				success++
				require.Equal(t, int64(4), o.BytesRx)
			} else {
				canceled++
				require.ErrorIs(t, o.Err, context.Canceled)
				// A response may race with cancellation; either way, bytes
				// from the other attempt must not enter this observation.
				require.LessOrEqual(t, o.BytesRx, int64(4))
			}
		case <-time.After(time.Second):
			t.Fatal("missing speculative observation")
		}
	}
	require.GreaterOrEqual(t, success, 1)
	require.Equal(t, 2, success+canceled)
}

func TestObserveByteMetricsPages(t *testing.T) {
	var mu sync.Mutex
	var page int
	var expected []byteCounts
	srv := newTestServerOpts{
		addr: "127.0.0.1:0", protocol: protoVersion4,
		customRequestHandler: func(_ *TestServer, req, resp *framer) error {
			switch req.header.op {
			case opStartup:
				resp.writeHeader(0, opReady, req.header.stream)
			case opOptions:
				resp.writeHeader(0, opSupported, req.header.stream)
				resp.writeShort(0)
			case opQuery:
				page++
				resp.writeHeader(0, opResult, req.header.stream)
				resp.writeInt(resultKindRows)
				flags := flagGlobalTableSpec
				if page == 1 {
					flags |= flagHasMorePages
				}
				resp.writeInt(int32(flags))
				resp.writeInt(1) // one column
				if page == 1 {
					resp.writeBytes([]byte{1})
				}
				resp.writeString("keyspace")
				resp.writeString("table")
				resp.writeString("value")
				resp.writeShort(uint16(TypeInt))
				resp.writeInt(1) // one row
				resp.writeBytes([]byte{0, 0, 0, byte(page)})
				mu.Lock()
				expected = append(expected, byteCounts{int64(req.header.length), int64(len(resp.buf) - frameHeadSize)})
				mu.Unlock()
			default:
				return fmt.Errorf("unexpected opcode %v", req.header.op)
			}
			return nil
		},
	}.newServer(t, context.Background())
	defer srv.Stop()
	cluster := testCluster(protoVersion4, srv.Address)
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	require.NoError(t, err)
	defer db.Close()
	var events []ObservedQuery
	observer := unitFuncQueryObserver(func(_ context.Context, o ObservedQuery) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, o)
	})
	iter := db.Query("paged").PageSize(1).Observer(observer).Iter()
	var value int
	require.True(t, iter.Scan(&value))
	require.Equal(t, 1, value)
	require.True(t, iter.Scan(&value))
	require.Equal(t, 2, value)
	require.False(t, iter.Scan(&value))
	require.NoError(t, iter.Close())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, events, 2)
	require.Len(t, expected, 2)
	for i, o := range events {
		require.Zero(t, o.Attempt)
		// Each page observation counts only its own request and response.
		require.Equal(t, expected[i], byteCounts{o.BytesTx, o.BytesRx})
	}
}

func TestObserveByteMetricsSchemaAgreement(t *testing.T) {
	var mu sync.Mutex
	var expected byteCounts
	var queries int
	srv := newTestServerOpts{
		addr: "127.0.0.1:0", protocol: protoVersion4,
		customRequestHandler: func(_ *TestServer, req, resp *framer) error {
			switch req.header.op {
			case opStartup:
				resp.writeHeader(0, opReady, req.header.stream)
			case opOptions:
				resp.writeHeader(0, opSupported, req.header.stream)
				resp.writeShort(0)
			case opQuery:
				statement, err := req.readLongString()
				if err != nil {
					return err
				}
				resp.writeHeader(0, opResult, req.header.stream)
				if statement == "ddl" {
					resp.writeInt(resultKindSchemaChanged)
					resp.writeString("CREATED")
					resp.writeString("KEYSPACE")
					resp.writeString("keyspace")
				} else {
					resp.writeInt(resultKindRows)
					resp.writeInt(0) // metadata flags
					resp.writeInt(0) // columns
					resp.writeInt(0) // rows; no conflicting schema versions
				}
				mu.Lock()
				queries++
				expected.bytesTx += int64(req.header.length)
				expected.bytesRx += int64(len(resp.buf) - frameHeadSize)
				mu.Unlock()
			default:
				return fmt.Errorf("unexpected opcode %v", req.header.op)
			}
			return nil
		},
	}.newServer(t, context.Background())
	defer srv.Stop()
	db, err := newTestSession(protoVersion4, srv.Address)
	require.NoError(t, err)
	defer db.Close()
	var observed ObservedQuery
	var events int
	observer := unitFuncQueryObserver(func(_ context.Context, o ObservedQuery) { observed = o; events++ })
	require.NoError(t, db.Query("ddl").Observer(observer).Exec())
	require.Equal(t, 1, events)
	mu.Lock()
	defer mu.Unlock()
	// The DDL exchange and the peer/local schema queries belong to one attempt.
	require.Equal(t, 3, queries)
	require.Equal(t, expected, byteCounts{observed.BytesTx, observed.BytesRx})
}

func newByteMetricsConn(t *testing.T, proto byte, compressor Compressor) (*Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); client.Close(); server.Close() })
	c := &Conn{
		r: &connReader{conn: client, r: bufio.NewReader(client)},
		w: &deadlineContextWriter{
			w: client, timeout: time.Second, semaphore: make(chan struct{}, 1), quit: make(chan struct{}),
		},
		version: proto, compressor: compressor, streams: streams.New(int(proto)),
		calls: make(map[int]*callReq), ctx: ctx, cancel: cancel,
		session: &Session{types: GlobalTypes}, requestTimeout: time.Second, host: &HostInfo{},
		logger: &defaultLogger{}, errorHandler: connErrorHandlerFn(func(*Conn, error, bool) {}),
	}
	return c, server
}

// readByteMetricsRequest reads one request as the server, returning its header
// and decompressed body.
func readByteMetricsRequest(r io.Reader, proto byte, compressor Compressor) (frameHeader, []byte, error) {
	if proto == protoVersion5 {
		var payload []byte
		var err error
		if compressor != nil {
			payload, _, err = readCompressedSegment(r, compressor)
		} else {
			payload, _, err = readUncompressedSegment(r)
		}
		if err != nil {
			return frameHeader{}, nil, err
		}
		r = bytes.NewReader(payload)
	}
	head, err := readHeader(r, make([]byte, frameHeadSize))
	if err != nil {
		return frameHeader{}, nil, err
	}
	request := newFramer(compressor, proto, GlobalTypes)
	if err := request.readFrame(r, &head); err != nil {
		return frameHeader{}, nil, err
	}
	return head, request.buf, nil
}

// newByteMetricsResponse builds a finished VOID result frame for stream, padded
// to bodySize bytes before compression.
func newByteMetricsResponse(proto byte, compressor Compressor, stream, bodySize int) (*framer, error) {
	f := newFramer(compressor, proto, GlobalTypes)
	f.writeHeader(f.flags, opResult, stream)
	f.buf[0] |= 0x80
	f.writeInt(resultKindVoid)
	f.buf = append(f.buf, make([]byte, bodySize-4)...)
	return f, f.finish()
}

func TestByteMetricsExec(t *testing.T) {
	for _, tc := range []struct {
		name       string
		proto      byte
		compressor Compressor
		bodySize   int
	}{
		{"v4-plain", protoVersion4, nil, 2048},
		{"v4-snappy", protoVersion4, snappy.SnappyCompressor{}, 2048},
		{"v5-plain", protoVersion5, nil, 2048},
		{"v5-lz4", protoVersion5, lz4.LZ4Compressor{}, 2048},
		// Responses larger than one segment are reassembled from several.
		{"v5-plain-fragmented", protoVersion5, nil, 2 * maxSegmentPayloadSize},
		{"v5-lz4-fragmented", protoVersion5, lz4.LZ4Compressor{}, 2 * maxSegmentPayloadSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, server := newByteMetricsConn(t, tc.proto, tc.compressor)
			metrics := &byteMetrics{}
			type result struct {
				counts byteCounts
				err    error
			}
			serverResult := make(chan result, 1)
			go func() {
				head, body, err := readByteMetricsRequest(server, tc.proto, tc.compressor)
				var response *framer
				if err == nil {
					response, err = newByteMetricsResponse(tc.proto, tc.compressor, head.stream, tc.bodySize)
				}
				if err == nil && tc.proto == protoVersion5 {
					err = response.prepareModernLayout()
				}
				if err == nil {
					_, err = server.Write(response.buf)
				}
				// Bodies as they were before compression and after decompression.
				serverResult <- result{counts: byteCounts{int64(len(body)), int64(tc.bodySize)}, err: err}
			}()
			received := make(chan error, 1)
			go func() { received <- c.recv(c.ctx, true) }()
			_, err := c.execWithMetrics(c.ctx, &writeQueryFrame{
				statement: strings.Repeat("query ", 256), params: queryParams{consistency: One},
			}, nil, metrics)
			require.NoError(t, err)
			require.NoError(t, <-received)
			actual := <-serverResult
			require.NoError(t, actual.err)
			require.Equal(t, actual.counts, metrics.snapshot())
		})
	}
}

func TestByteMetricsSharedSegment(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			var compressor Compressor
			if compressed {
				compressor = lz4.LZ4Compressor{}
			}
			c, server := newByteMetricsConn(t, protoVersion5, compressor)
			statements := []string{"a", strings.Repeat("b", 29), strings.Repeat("c", 113)}
			metrics := make([]*byteMetrics, len(statements))
			execErrs := make(chan error, len(statements))
			for i, statement := range statements {
				metrics[i] = &byteMetrics{}
				go func(statement string, m *byteMetrics) {
					_, err := c.execWithMetrics(c.ctx, &writeQueryFrame{
						statement: statement, params: queryParams{consistency: One},
					}, nil, m)
					execErrs <- err
				}(statement, metrics[i])
			}
			// Answer every request in one self-contained segment. Each response
			// body is 4 bytes longer than its request body, so each attempt can
			// be checked against its own request.
			var payload []byte
			for range statements {
				head, body, err := readByteMetricsRequest(server, protoVersion5, compressor)
				require.NoError(t, err)
				response, err := newByteMetricsResponse(protoVersion5, nil, head.stream, len(body)+4)
				require.NoError(t, err)
				payload = append(payload, response.buf...)
			}
			var segment []byte
			var err error
			if compressed {
				segment, err = newCompressedSegment(payload, true, compressor)
			} else {
				segment, err = newUncompressedSegment(payload, true)
			}
			require.NoError(t, err)
			received := make(chan error, 1)
			go func() { received <- c.recv(c.ctx, true) }()
			_, err = server.Write(segment)
			require.NoError(t, err)
			require.NoError(t, <-received)
			for range statements {
				require.NoError(t, <-execErrs)
			}
			for _, m := range metrics {
				counts := m.snapshot()
				require.Positive(t, counts.bytesTx)
				// Each response is charged only its own body, not the shared segment.
				require.Equal(t, counts.bytesTx+4, counts.bytesRx)
			}
		})
	}
}

// TestByteMetricsLateResponse checks that a response read after its request
// timed out is not counted, even though the stream is still waiting for it.
func TestByteMetricsLateResponse(t *testing.T) {
	c, server := newByteMetricsConn(t, protoVersion4, nil)
	c.requestTimeout = 10 * time.Millisecond
	metrics := &byteMetrics{}
	execErr := make(chan error, 1)
	go func() {
		_, err := c.execWithMetrics(c.ctx, &writeQueryFrame{statement: "query", params: queryParams{consistency: One}}, nil, metrics)
		execErr <- err
	}()
	head, body, err := readByteMetricsRequest(server, protoVersion4, nil)
	require.NoError(t, err)
	require.ErrorIs(t, <-execErr, ErrTimeoutNoResponse)

	response, err := newByteMetricsResponse(protoVersion4, nil, head.stream, 4)
	require.NoError(t, err)
	received := make(chan error, 1)
	go func() { received <- c.recv(c.ctx, true) }()
	_, err = server.Write(response.buf)
	require.NoError(t, err)
	require.NoError(t, <-received)
	require.Equal(t, byteCounts{bytesTx: int64(len(body))}, metrics.snapshot())
}

type partialByteMetricsWriter struct{}

func (partialByteMetricsWriter) writeContext(context.Context, []byte) (int, error) {
	return 5, io.ErrShortWrite
}

func TestByteMetricsPartialWrite(t *testing.T) {
	c, _ := newByteMetricsConn(t, protoVersion4, nil)
	c.w = partialByteMetricsWriter{}
	metrics := &byteMetrics{}
	_, err := c.execWithMetrics(c.ctx, &writeQueryFrame{statement: "query", params: queryParams{consistency: One}}, nil, metrics)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, byteCounts{}, metrics.snapshot())
}

func TestByteMetricsPartialRead(t *testing.T) {
	c, server := newByteMetricsConn(t, protoVersion4, nil)
	metrics := &byteMetrics{}
	execErr := make(chan error, 1)
	go func() {
		_, err := c.execWithMetrics(c.ctx, &writeQueryFrame{statement: "query", params: queryParams{consistency: One}}, nil, metrics)
		execErr <- err
	}()
	head, body, err := readByteMetricsRequest(server, protoVersion4, nil)
	require.NoError(t, err)

	response, err := newByteMetricsResponse(protoVersion4, nil, head.stream, 24)
	require.NoError(t, err)
	received := make(chan error, 1)
	go func() { received <- c.recv(c.ctx, true) }()
	// Send the header and only part of the body before the connection closes.
	_, err = server.Write(response.buf[:frameHeadSize+5])
	require.NoError(t, err)
	require.NoError(t, server.Close())
	require.NoError(t, <-received)
	require.Error(t, <-execErr)
	require.Equal(t, byteCounts{bytesTx: int64(len(body))}, metrics.snapshot())
}
