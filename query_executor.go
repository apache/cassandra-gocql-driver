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
	"sync"
	"time"
)

type ExecutableQuery interface {
	borrowForExecution()    // Used to ensure that the query stays alive for lifetime of a particular execution goroutine.
	releaseAfterExecution() // Used when a goroutine finishes its execution attempts, either with ok result or an error.
	execute(ctx context.Context, conn *Conn, iter *Iter) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retryPolicy() RetryPolicy
	speculativeExecutionPolicy() SpeculativeExecutionPolicy
	GetRoutingKey() ([]byte, error)
	Keyspace() string
	Table() string
	IsIdempotent() bool

	withContext(context.Context) ExecutableQuery
	Clone() ExecutableQuery

	SetConsistency(c Consistency)
	GetConsistency() Consistency
	Context() context.Context
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy
}

func (q *queryExecutor) attemptQuery(ctx context.Context, qry ExecutableQuery, it *Iter, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(ctx, conn, it)
	end := time.Now()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

func (q *queryExecutor) speculate(ctx context.Context, qry ExecutableQuery, sp SpeculativeExecutionPolicy,
	hostIter NextHost, results chan *Iter) *Iter {
	ticker := time.NewTicker(sp.Delay())
	defer ticker.Stop()

	qry = qry.Clone()
	for i := 0; i < sp.Attempts(); i++ {
		select {
		case <-ticker.C:
			qry.borrowForExecution() // ensure liveness in case of executing Query to prevent races with Query.Release().
			go q.run(ctx, qry, nil, hostIter, results)
		case <-ctx.Done():
			return NewIterErr(qry, ctx.Err())
		case iter := <-results:
			return iter
		}
	}

	return nil
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery, it *Iter) (*Iter, error) {
	hostIter := q.policy.Pick(qry)

	// check if the query is not marked as idempotent, if
	// it is, we force the policy to NonSpeculative
	sp := qry.speculativeExecutionPolicy()
	if !qry.IsIdempotent() || sp.Attempts() == 0 {
		return q.do(qry.Context(), qry, it, hostIter), nil
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
	go q.run(ctx, qry, it, hostIter, results)

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
		return NewIterErr(qry, ctx.Err()), nil
	}
}

func (q *queryExecutor) do(ctx context.Context, qry ExecutableQuery, it *Iter, hostIter NextHost) *Iter {
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

		ni := q.attemptQuery(ctx, qry, it, conn)
		ni.merge(iter)
		iter = ni
		iter.host = selectedHost.Info()
		// Update host
		switch iter.err {
		case context.Canceled, context.DeadlineExceeded, ErrNotFound:
			// those errors represent logical errors, they should not count
			// toward removing a node from the pool
			selectedHost.Mark(nil)
			return iter
		default:
			selectedHost.Mark(iter.err)
		}

		// Exit if the query was successful
		// or query is not idempotent or no retry policy defined
		if iter.err == nil || !qry.IsIdempotent() || rt == nil {
			return iter
		}

		// clone to make the query attributes updatable by retry policy and original immutable
		iter.qry = qry.Clone()
		attemptsReached := !rt.Attempt(iter)
		retryType := rt.GetRetryType(iter.err)

		var stopRetries bool
		// If query is unsuccessful, check the error with RetryPolicy to retry
		switch retryType {
		case Retry:
			// retry on the same host
		case RetryNextHost:
			// retry on the next host
			selectedHost = hostIter()
		case Ignore:
			iter.err = nil
			stopRetries = true
		case Rethrow:
			stopRetries = true
		default:
			// Undefined? Return nil and error, this will panic in the requester
			return NewIterErrFromIter(qry, ErrUnknownRetryType, iter)
		}

		if stopRetries || attemptsReached {
			return iter
		}

		qry = iter.qry
		lastErr = iter.err
		continue
	}

	if lastErr != nil {
		return NewIterErrFromIter(qry, lastErr, iter)
	}

	return NewIterErrFromIter(qry, ErrNoConnections, iter)
}

func (q *queryExecutor) run(ctx context.Context, qry ExecutableQuery, it *Iter, hostIter NextHost, results chan<- *Iter) {
	select {
	case results <- q.do(ctx, qry, it, hostIter):
	case <-ctx.Done():
	}
	qry.releaseAfterExecution()
}
