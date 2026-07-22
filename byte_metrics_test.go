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
							expected.bytesTx += frameHeadSize + req.header.length
							expected.uncompressedBytesTx += req.header.length
							expected.bytesRx += len(resp.buf)
							expected.uncompressedBytesRx += len(resp.buf) - frameHeadSize
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
						observed = byteCounts{o.BytesTx, o.BytesRx, o.UncompressedBytesTx, o.UncompressedBytesRx}
					}))
					execute = b.Exec
				} else {
					q := db.Query("select value from table").RoutingKey([]byte{1})
					q.Observer(unitFuncQueryObserver(func(_ context.Context, o ObservedQuery) {
						events++
						observed = byteCounts{o.BytesTx, o.BytesRx, o.UncompressedBytesTx, o.UncompressedBytesRx}
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
		require.Equal(t, o.UncompressedBytesTx+frameHeadSize, o.BytesTx)
		require.Equal(t, o.UncompressedBytesRx+frameHeadSize, o.BytesRx)
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
			require.Positive(t, o.UncompressedBytesTx)
			require.Equal(t, o.UncompressedBytesTx+frameHeadSize, o.BytesTx)
			if o.Err == nil {
				success++
				require.Equal(t, 4, o.UncompressedBytesRx)
				require.Equal(t, frameHeadSize+4, o.BytesRx)
			} else {
				canceled++
				require.ErrorIs(t, o.Err, context.Canceled)
				// A response may race with cancellation; either way, bytes
				// from the other attempt must not enter this observation.
				require.LessOrEqual(t, o.BytesRx, frameHeadSize+4)
				require.LessOrEqual(t, o.UncompressedBytesRx, 4)
			}
		case <-time.After(time.Second):
			t.Fatal("missing speculative observation")
		}
	}
	require.GreaterOrEqual(t, success, 1)
	require.Equal(t, 2, success+canceled)
}

func TestObserveByteMetricsPages(t *testing.T) {
	var page int
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
	var mu sync.Mutex
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
	for _, o := range events {
		require.Zero(t, o.Attempt)
		require.Equal(t, o.UncompressedBytesTx+frameHeadSize, o.BytesTx)
		require.Equal(t, o.UncompressedBytesRx+frameHeadSize, o.BytesRx)
	}
}

func TestObserveByteMetricsSchemaAgreement(t *testing.T) {
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
	// The DDL exchange and the peer/local schema queries belong to one attempt.
	require.Equal(t, observed.UncompressedBytesTx+3*frameHeadSize, observed.BytesTx)
	require.Equal(t, observed.UncompressedBytesRx+3*frameHeadSize, observed.BytesRx)
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
		session: &Session{types: GlobalTypes}, requestTimeout: time.Second,
		logger: &defaultLogger{}, errorHandler: connErrorHandlerFn(func(*Conn, error, bool) {}),
	}
	return c, server
}

func TestByteMetricsCompression(t *testing.T) {
	for _, tc := range []struct {
		name       string
		proto      byte
		compressor Compressor
	}{
		{"v4-plain", protoVersion4, nil},
		{"v4-snappy", protoVersion4, snappy.SnappyCompressor{}},
		{"v5-plain", protoVersion5, nil},
		{"v5-lz4", protoVersion5, lz4.LZ4Compressor{}},
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
				reader := &countingReader{Reader: server}
				var r io.Reader = reader
				if tc.proto == protoVersion5 {
					var payload []byte
					var err error
					if tc.compressor != nil {
						payload, _, err = readCompressedSegment(reader, tc.compressor)
					} else {
						payload, _, err = readUncompressedSegment(reader)
					}
					if err != nil {
						serverResult <- result{err: err}
						return
					}
					r = bytes.NewReader(payload)
				}
				head, err := readHeader(r, make([]byte, frameHeadSize))
				if err != nil {
					serverResult <- result{err: err}
					return
				}
				request := newFramer(tc.compressor, tc.proto, GlobalTypes)
				if err = request.readFrame(r, &head); err != nil {
					serverResult <- result{err: err}
					return
				}
				response := newFramer(tc.compressor, tc.proto, GlobalTypes)
				response.writeHeader(response.flags, opResult, head.stream)
				response.buf[0] |= 0x80
				response.writeInt(resultKindVoid)
				response.buf = append(response.buf, bytes.Repeat([]byte("response"), 256)...)
				bodySize := len(response.buf) - frameHeadSize
				if err = response.finish(); err == nil && tc.proto == protoVersion5 {
					err = response.prepareModernLayout()
				}
				if err == nil {
					_, err = server.Write(response.buf)
				}
				serverResult <- result{counts: byteCounts{
					reader.n, len(response.buf), len(request.buf), bodySize,
				}, err: err}
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
			if tc.compressor != nil {
				require.Less(t, actual.counts.bytesTx, actual.counts.uncompressedBytesTx)
				require.Less(t, actual.counts.bytesRx, actual.counts.uncompressedBytesRx)
			}
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
			var payload []byte
			var calls []*callReq
			bodySizes := []int{4, 29, 113}
			for i, size := range bodySizes {
				f := newFramer(nil, protoVersion5, GlobalTypes)
				f.writeHeader(0, opResult, i+1)
				f.buf[0] |= 0x80
				f.buf = append(f.buf, bytes.Repeat([]byte{byte(i)}, size)...)
				require.NoError(t, f.finish())
				payload = append(payload, f.buf...)
				call := &callReq{streamID: i + 1, timeout: make(chan struct{}), resp: make(chan callResp, 1), bytes: &byteMetrics{}}
				c.calls[i+1] = call
				calls = append(calls, call)
			}
			var segment []byte
			var err error
			if compressed {
				segment, err = newCompressedSegment(payload, true, compressor)
			} else {
				segment, err = newUncompressedSegment(payload, true)
			}
			require.NoError(t, err)
			written := make(chan error, 1)
			go func() { _, err := server.Write(segment); written <- err }()
			require.NoError(t, c.recvSegment(c.ctx))
			require.NoError(t, <-written)
			var allocated, consumed int
			for i, call := range calls {
				resp := <-call.resp
				require.NoError(t, resp.err)
				counts := call.bytes.snapshot()
				require.Equal(t, bodySizes[i], counts.uncompressedBytesRx)
				consumed += frameHeadSize + bodySizes[i]
				// The cumulative fraction also accounts for integer rounding.
				want := consumed*len(segment)/len(payload) - allocated
				require.Equal(t, want, counts.bytesRx)
				allocated += counts.bytesRx
			}
			require.Equal(t, len(segment), allocated)
		})
	}
}

func TestByteMetricsFragmentedResponse(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			var compressor Compressor
			if compressed {
				compressor = lz4.LZ4Compressor{}
			}
			c, server := newByteMetricsConn(t, protoVersion5, compressor)
			metrics := &byteMetrics{}
			call := &callReq{streamID: 1, timeout: make(chan struct{}), resp: make(chan callResp, 1), bytes: metrics}
			c.calls[1] = call
			f := newFramer(compressor, protoVersion5, GlobalTypes)
			f.writeHeader(0, opResult, 1)
			f.buf[0] |= 0x80
			body := bytes.Repeat([]byte("fragmented"), maxSegmentPayloadSize/5)
			f.buf = append(f.buf, body...)
			require.NoError(t, f.finish())
			require.NoError(t, f.prepareModernLayout())
			written := make(chan error, 1)
			go func() { _, err := server.Write(f.buf); written <- err }()
			require.NoError(t, c.recvSegment(c.ctx))
			require.NoError(t, <-written)
			resp := <-call.resp
			require.NoError(t, resp.err)
			require.Equal(t, body, resp.framer.buf)
			require.Equal(t, len(f.buf), metrics.snapshot().bytesRx)
			require.Equal(t, len(body), metrics.snapshot().uncompressedBytesRx)
		})
	}
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
	require.Equal(t, byteCounts{bytesTx: 5}, metrics.snapshot())
}

func TestByteMetricsPartialRead(t *testing.T) {
	c, _ := newByteMetricsConn(t, protoVersion4, nil)
	metrics := &byteMetrics{}
	call := &callReq{streamID: 1, timeout: make(chan struct{}), resp: make(chan callResp, 1), bytes: metrics}
	c.calls[1] = call
	f := newFramer(nil, protoVersion4, GlobalTypes)
	f.writeHeader(0, opResult, 1)
	f.buf[0] |= 0x80
	f.buf = append(f.buf, make([]byte, 20)...)
	require.NoError(t, f.finish())
	require.NoError(t, c.processFrame(c.ctx, bytes.NewReader(f.buf[:frameHeadSize+5])))
	require.Error(t, (<-call.resp).err)
	require.Equal(t, byteCounts{bytesRx: frameHeadSize + 5}, metrics.snapshot())
}

func TestByteMetricsReadSnapshotBeforeResponseCompletes(t *testing.T) {
	c, server := newByteMetricsConn(t, protoVersion4, nil)
	metrics := &byteMetrics{}
	call := &callReq{streamID: 1, timeout: make(chan struct{}), resp: make(chan callResp, 1), bytes: metrics}
	c.calls[1] = call
	f := newFramer(nil, protoVersion4, GlobalTypes)
	f.writeHeader(0, opResult, 1)
	f.buf[0] |= 0x80
	f.buf = append(f.buf, make([]byte, 20)...)
	require.NoError(t, f.finish())
	finished := make(chan error, 1)
	go func() { finished <- c.processFrame(c.ctx, c.r) }()
	_, err := server.Write(f.buf[:frameHeadSize+5])
	require.NoError(t, err)
	// The rest of the response has not arrived, but these bytes have already
	// been read and must be available to a canceled attempt's observer.
	require.Eventually(t, func() bool {
		return metrics.snapshot() == (byteCounts{bytesRx: frameHeadSize + 5})
	}, time.Second, time.Millisecond)
	require.NoError(t, server.Close())
	require.NoError(t, <-finished)
	require.Error(t, (<-call.resp).err)
}
