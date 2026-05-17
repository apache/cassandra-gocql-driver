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
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestExecuteBatch_UnprepRetryIsCapped verifies that Conn.executeBatch
// stops re-preparing after maxUnprepRetries when the server returns
// ErrCodeUnprepared on every batch attempt.
//
// Mirrors TestExecuteQuery_UnprepRetryIsCapped for the batch path. Without
// the cap, executeBatch would recurse indefinitely on a server-side cache
// thrash (Cassandra evicting our prepared statement between attempts) and
// stack-overflow the goroutine.
//
// The fake server's opPrepare handler returns id=99 for "always-unprep".
// Its opBatch handler returns ErrCodeUnprepared with id=99 whenever any
// statement in the batch carries that id. Each driver retry: evict cache,
// re-prepare (server gives 99 again), send batch (server says unprepared)
// — loops forever absent the cap.
func TestExecuteBatch_UnprepRetryIsCapped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var prepCount, batchCount uint64
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			switch f.header.op {
			case opPrepare:
				atomic.AddUint64(&prepCount, 1)
			case opBatch:
				atomic.AddUint64(&batchCount, 1)
			}
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatch(LoggedBatch)
	// Use Bind with an empty values slice so the batch entry goes through
	// prepareStatement (binding != nil) but the post-prepare arity check
	// matches the fake server's always-unprep prepared response, which
	// declares 0 request columns.
	b.Bind("insert always-unprep into x (k) values (?)", func(*QueryInfo) ([]interface{}, error) {
		return nil, nil
	})
	err = db.ExecuteBatch(b)
	if err == nil {
		t.Fatalf("expected re-prepare cap error, got nil")
	}

	if !strings.Contains(err.Error(), "re-prepare attempts") {
		t.Errorf("error %q does not mention re-prepare attempts; cap behavior may be missing", err)
	}

	var serverErr *RequestErrUnprepared
	if !errors.As(err, &serverErr) {
		t.Errorf("errors.As(err, *RequestErrUnprepared) = false; %%w not in effect")
	}

	time.Sleep(50 * time.Millisecond)

	wantPairs := uint64(maxUnprepRetries + 1)
	gotPrep := atomic.LoadUint64(&prepCount)
	gotBatch := atomic.LoadUint64(&batchCount)
	if gotPrep != wantPairs {
		t.Errorf("prepare count = %d, want %d (cap=%d allows %d retries plus initial)",
			gotPrep, wantPairs, maxUnprepRetries, maxUnprepRetries)
	}
	if gotBatch != wantPairs {
		t.Errorf("batch count = %d, want %d", gotBatch, wantPairs)
	}
}

// TestExecuteQuery_UnprepRetryIsCapped verifies that Conn.executeQuery
// stops re-preparing after maxUnprepRetries when the server returns
// ErrCodeUnprepared on every Execute attempt.
//
// Without the cap, a server-side prepared-statement cache thrash
// (evicting the driver's statement between every attempt) would cause
// unbounded recursion in executeQuery and stack-overflow the goroutine.
// This test drives that exact scenario via the fake test server.
//
// The server's opPrepare handler for "always-unprep" returns id=99 each
// time. The opExecute default branch returns ErrCodeUnprepared for any
// id != 1 and != 2. Each driver retry: evict, re-prepare (server gives
// 99 again), execute (server says unprepared) — loops forever absent
// the cap.
func TestExecuteQuery_UnprepRetryIsCapped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var prepCount, execCount uint64
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			switch f.header.op {
			case opPrepare:
				atomic.AddUint64(&prepCount, 1)
			case opExecute:
				atomic.AddUint64(&execCount, 1)
			}
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// "select always-unprep ..." routes through the fake server's
	// opPrepare always-unprep case. shouldPrepare requires a SELECT/INSERT
	// keyword prefix so we name the query accordingly.
	err = db.Query("select always-unprep from x").Exec()
	if err == nil {
		t.Fatalf("expected re-prepare cap error, got nil")
	}

	// The wrap message must mention the attempt count so operators can
	// diagnose the server-side cache thrash.
	if !strings.Contains(err.Error(), "re-prepare attempts") {
		t.Errorf("error %q does not mention re-prepare attempts; cap behavior may be missing", err)
	}

	// errors.As must recover the underlying server error.
	var serverErr *RequestErrUnprepared
	if !errors.As(err, &serverErr) {
		t.Errorf("errors.As(err, *RequestErrUnprepared) = false; %%w not in effect")
	}

	// Wait for any in-flight goroutines to settle. The exec/prep counters
	// can lag the test if the server logged the request just before we
	// read the counter.
	time.Sleep(50 * time.Millisecond)

	// We should have seen exactly maxUnprepRetries+1 prepare-execute
	// pairs: the initial attempt, plus N retries.
	wantPairs := uint64(maxUnprepRetries + 1)
	gotPrep := atomic.LoadUint64(&prepCount)
	gotExec := atomic.LoadUint64(&execCount)
	if gotPrep != wantPairs {
		t.Errorf("prepare count = %d, want %d (cap=%d allows %d retries plus initial)",
			gotPrep, wantPairs, maxUnprepRetries, maxUnprepRetries)
	}
	if gotExec != wantPairs {
		t.Errorf("execute count = %d, want %d", gotExec, wantPairs)
	}
}
