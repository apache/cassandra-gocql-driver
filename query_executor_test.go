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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

type executorTestRequest struct {
	*internalQuery
	executeFunc func(context.Context, *Conn) *Iter
}

func (r *executorTestRequest) execute(ctx context.Context, conn *Conn) *Iter {
	return r.executeFunc(ctx, conn)
}

func singleExecutorTestHost(host *HostInfo) NextHost {
	return func() SelectedHost {
		if host == nil {
			return nil
		}
		next := host
		host = nil
		return (*selectedHost)(next)
	}
}

func TestQueryExecutorRunWithoutAttempt(t *testing.T) {
	for _, test := range []struct {
		name string
		host *HostInfo
		pool *hostConnPool
	}{
		{name: "no host"},
		{name: "host down", host: &HostInfo{hostId: "host", state: NodeDown}},
		{name: "missing pool", host: &HostInfo{hostId: "host", state: NodeUp}},
		{name: "no connection", host: &HostInfo{hostId: "host", state: NodeUp}, pool: &hostConnPool{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, execution := range []struct {
				name        string
				speculative bool
			}{
				{name: "primary"},
				{name: "speculative", speculative: true},
			} {
				t.Run(execution.name, func(t *testing.T) {
					ctx := context.Background()
					request := &executorTestRequest{
						internalQuery: newInternalQuery(&Query{keyspace: "test"}, ctx),
						executeFunc: func(context.Context, *Conn) *Iter {
							t.Fatal("an unavailable host must not execute the request")
							return nil
						},
					}
					pools := make(map[string]*hostConnPool)
					if test.pool != nil {
						pools[test.host.HostID()] = test.pool
					}
					executor := &queryExecutor{pool: &policyConnPool{hostConnPools: pools}}
					results := make(chan *Iter, 1)
					executor.run(ctx, request, singleExecutorTestHost(test.host), results, execution.speculative)

					select {
					case iter := <-results:
						require.False(t, execution.speculative, "an unstarted speculative execution must not publish a result")
						require.ErrorIs(t, iter.err, ErrNoConnections)
					default:
						require.True(t, execution.speculative, "the primary must still publish its failure")
					}
					require.Zero(t, request.Attempts())
				})
			}
		})
	}
}

func TestQueryExecutorRunPublishesAttemptedErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		exhaustion bool
	}{
		{name: "no connections error from attempt", err: ErrNoConnections},
		{name: "timeout from attempt", err: ErrTimeoutNoResponse},
		{name: "no connections after retry exhaustion", err: ErrNoConnections, exhaustion: true},
		{name: "timeout after retry exhaustion", err: ErrTimeoutNoResponse, exhaustion: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			query := &Query{keyspace: "test", idempotent: true}
			if test.exhaustion {
				query.rt = &SimpleRetryPolicy{NumRetries: 2}
			}
			request := &executorTestRequest{internalQuery: newInternalQuery(query, ctx)}
			request.executeFunc = func(context.Context, *Conn) *Iter {
				return newErrIter(test.err, request.metrics, "test", request.routingInfo, nil)
			}
			host := &HostInfo{hostId: "host", state: NodeUp}
			conn := &Conn{host: host, streams: streams.New(4)}
			executor := &queryExecutor{pool: &policyConnPool{hostConnPools: map[string]*hostConnPool{
				"host": {size: 1, conns: []*Conn{conn}},
			}}}
			results := make(chan *Iter, 1)
			executor.run(ctx, request, singleExecutorTestHost(host), results, true)

			select {
			case iter := <-results:
				require.ErrorIs(t, iter.err, test.err)
				if test.exhaustion {
					// Exhausting the retry plan creates an iterator with no host,
					// even though this execution already made a real attempt.
					require.Nil(t, iter.host)
				} else {
					require.Same(t, host, iter.host)
				}
			default:
				t.Fatal("an attempted execution must publish its error")
			}
			require.Equal(t, 1, request.Attempts())
		})
	}
}

func TestQueryExecutorRunCancellationWithoutResultReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	started := make(chan struct{})
	request := &executorTestRequest{internalQuery: newInternalQuery(&Query{keyspace: "test"}, ctx)}
	request.executeFunc = func(ctx context.Context, _ *Conn) *Iter {
		close(started)
		<-ctx.Done()
		return newErrIter(ctx.Err(), request.metrics, "test", request.routingInfo, nil)
	}
	host := &HostInfo{hostId: "host", state: NodeUp}
	conn := &Conn{host: host, streams: streams.New(4)}
	executor := &queryExecutor{pool: &policyConnPool{hostConnPools: map[string]*hostConnPool{
		"host": {size: 1, conns: []*Conn{conn}},
	}}}
	results := make(chan *Iter)
	done := make(chan struct{})
	go func() {
		defer close(done)
		executor.run(ctx, request, singleExecutorTestHost(host), results, true)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled execution blocked trying to publish its result")
	}
	require.Equal(t, 1, request.Attempts())
}
