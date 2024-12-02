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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"net"
	"sync"
	"time"
)

type ExecutableQuery interface {
	borrowForExecution()    // Used to ensure that the query stays alive for lifetime of a particular execution goroutine.
	releaseAfterExecution() // Used when a goroutine finishes its execution attempts, either with ok result or an error.
	execute(ctx context.Context, conn *Conn) *Iter
	attempt(ctx context.Context, keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retryPolicy() RetryPolicy
	speculativeExecutionPolicy() SpeculativeExecutionPolicy
	GetRoutingKey() ([]byte, error)
	Keyspace() string
	Table() string
	IsIdempotent() bool

	withContext(context.Context) ExecutableQuery

	RetryableQuery
}

type queryExecutor struct {
	pool        *policyConnPool
	policy      HostSelectionPolicy
	interceptor QueryAttemptInterceptor
}

type QueryAttempt struct {
	// The query to execute, either a *gocql.Query or *gocql.Batch.
	Query ExecutableQuery
	// The host that will receive the query.
	Host *HostInfo
	// The local address of the connection used to execute the query.
	LocalAddr net.Addr
	// The remote address of the connection used to execute the query.
	RemoteAddr net.Addr
	// The number of previous query attempts. 0 for the initial attempt, 1 for the first retry, etc.
	Attempts int
}

// QueryAttemptHandler is a function that attempts query execution.
type QueryAttemptHandler = func(context.Context, QueryAttempt) (*Iter, error)

// QueryAttemptInterceptor is the interface implemented by query interceptors / middleware.
//
// Interceptors are well-suited to logic that is not specific to a single query or batch.
type QueryAttemptInterceptor interface {
	// Intercept is invoked once immediately before a query execution attempt, including retry attempts and
	// speculative execution attempts.

	// The interceptor is responsible for calling the `handler` function and returning the handler result. If the
	// interceptor wants to bypass the handler and skip query execution, it should return an error. Failure to
	// return either the handler result or an error will panic.
	Intercept(ctx context.Context, attempt QueryAttempt, handler QueryAttemptHandler) (*Iter, error)
}

func (q *queryExecutor) attemptQuery(ctx context.Context, qry ExecutableQuery, conn *Conn) *Iter {
	start := time.Now()
	var iter *Iter
	var err error
	if q.interceptor != nil {
		// Propagate interceptor context modifications.
		_ctx := ctx
		attempt := QueryAttempt{
			Query:      qry,
			Host:       conn.host,
			LocalAddr:  conn.conn.LocalAddr(),
			RemoteAddr: conn.conn.RemoteAddr(),
			Attempts:   qry.Attempts(),
		}
		iter, err = q.interceptor.Intercept(_ctx, attempt, func(_ctx context.Context, attempt QueryAttempt) (*Iter, error) {
			ctx = _ctx
			iter := attempt.Query.execute(ctx, conn)
			return iter, iter.err
		})
		if err != nil {
			iter = &Iter{err: err}
		}
	} else {
		iter = qry.execute(ctx, conn)
	}

	end := time.Now()
	qry.attempt(ctx, q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

func (q *queryExecutor) speculate(ctx context.Context, qry ExecutableQuery, sp SpeculativeExecutionPolicy,
	hostIter NextHost, results chan *Iter) *Iter {
	ticker := time.NewTicker(sp.Delay())
	defer ticker.Stop()

	for i := 0; i < sp.Attempts(); i++ {
		select {
		case <-ticker.C:
			qry.borrowForExecution() // ensure liveness in case of executing Query to prevent races with Query.Release().
			go q.run(ctx, qry, hostIter, results)
		case <-ctx.Done():
			return &Iter{err: ctx.Err()}
		case iter := <-results:
			return iter
		}
	}

	return nil
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	hostIter := q.policy.Pick(qry)

	// check if the query is not marked as idempotent, if
	// it is, we force the policy to NonSpeculative
	sp := qry.speculativeExecutionPolicy()
	if !qry.IsIdempotent() || sp.Attempts() == 0 {
		return q.do(qry.Context(), qry, hostIter), nil
	}

	// When speculative execution is enabled, we could be accessing the host iterator from multiple goroutines below.
	// To ensure we don't call it concurrently, we wrap the returned NextHost function here to synchronize access to it.
	var mu sync.Mutex
	origHostIter := hostIter
	hostIter = func() SelectedHost {
		mu.Lock()
		defer mu.Unlock()
		return origHostIter()
	}

	ctx, cancel := context.WithCancel(qry.Context())
	defer cancel()

	results := make(chan *Iter, 1)

	// Launch the main execution
	qry.borrowForExecution() // ensure liveness in case of executing Query to prevent races with Query.Release().
	go q.run(ctx, qry, hostIter, results)

	// The speculative executions are launched _in addition_ to the main
	// execution, on a timer. So Speculation{2} would make 3 executions running
	// in total.
	if iter := q.speculate(ctx, qry, sp, hostIter, results); iter != nil {
		return iter, nil
	}

	select {
	case iter := <-results:
		return iter, nil
	case <-ctx.Done():
		return &Iter{err: ctx.Err()}, nil
	}
}

func (q *queryExecutor) do(ctx context.Context, qry ExecutableQuery, hostIter NextHost) *Iter {
	selectedHost := hostIter()
	rt := qry.retryPolicy()

	var lastErr error
	var iter *Iter
	for selectedHost != nil {
		host := selectedHost.Info()
		if host == nil || !host.IsUp() {
			selectedHost = hostIter()
			continue
		}

		pool, ok := q.pool.getPool(host)
		if !ok {
			selectedHost = hostIter()
			continue
		}

		conn := pool.Pick()
		if conn == nil {
			selectedHost = hostIter()
			continue
		}

		iter = q.attemptQuery(ctx, qry, conn)
		iter.host = selectedHost.Info()
		// Update host
		switch iter.err {
		case context.Canceled, context.DeadlineExceeded, ErrNotFound:
			// those errors represents logical errors, they should not count
			// toward removing a node from the pool
			selectedHost.Mark(nil)
			return iter
		default:
			selectedHost.Mark(iter.err)
		}

		// Exit if the query was successful
		// or no retry policy defined or retry attempts were reached
		if iter.err == nil || rt == nil || !rt.Attempt(qry) {
			return iter
		}
		lastErr = iter.err

		// If query is unsuccessful, check the error with RetryPolicy to retry
		switch rt.GetRetryType(iter.err) {
		case Retry:
			// retry on the same host
			continue
		case Rethrow, Ignore:
			return iter
		case RetryNextHost:
			// retry on the next host
			selectedHost = hostIter()
			continue
		default:
			// Undefined? Return nil and error, this will panic in the requester
			return &Iter{err: ErrUnknownRetryType}
		}
	}

	if lastErr != nil {
		return &Iter{err: lastErr}
	}

	return &Iter{err: ErrNoConnections}
}

func (q *queryExecutor) run(ctx context.Context, qry ExecutableQuery, hostIter NextHost, results chan<- *Iter) {
	select {
	case results <- q.do(ctx, qry, hostIter):
	case <-ctx.Done():
	}
	qry.releaseAfterExecution()
}
