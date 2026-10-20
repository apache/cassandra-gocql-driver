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
	"sync/atomic"
	"testing"
	"time"
)

// markCountingPolicy wraps a HostSelectionPolicy and counts Mark calls on the hosts it picks.
type markCountingPolicy struct {
	HostSelectionPolicy
	marks int32
}

func (p *markCountingPolicy) Pick(qry ExecutableStatement) NextHost {
	next := p.HostSelectionPolicy.Pick(qry)
	return func() SelectedHost {
		host := next()
		if host == nil {
			return nil
		}
		return markCountingHost{SelectedHost: host, marks: &p.marks}
	}
}

type markCountingHost struct {
	SelectedHost
	marks *int32
}

func (h markCountingHost) Mark(err error) {
	atomic.AddInt32(h.marks, 1)
	h.SelectedHost.Mark(err)
}

type rejectingInterceptor struct {
	err error
}

func (r rejectingInterceptor) InterceptPreAttempt(context.Context, RequestAttempt) (InterceptResult, error) {
	return InterceptResult{}, r.err
}

func (rejectingInterceptor) InterceptPostAttempt(InterceptResult, error) {}

// timingInterceptor records when its pre-attempt hook returns and its post-attempt hook is called.
type timingInterceptor struct {
	preDone   time.Time
	postStart time.Time
}

func (ti *timingInterceptor) InterceptPreAttempt(ctx context.Context, _ RequestAttempt) (InterceptResult, error) {
	ti.preDone = time.Now()
	return InterceptResult{Ctx: ctx}, nil
}

func (ti *timingInterceptor) InterceptPostAttempt(InterceptResult, error) {
	ti.postStart = time.Now()
}

type countingInterceptor struct {
	posts int
}

func (c *countingInterceptor) InterceptPreAttempt(ctx context.Context, _ RequestAttempt) (InterceptResult, error) {
	return InterceptResult{Ctx: ctx}, nil
}

func (c *countingInterceptor) InterceptPostAttempt(InterceptResult, error) {
	c.posts++
}

type lastQueryObserver struct {
	query ObservedQuery
}

func (o *lastQueryObserver) ObserveQuery(_ context.Context, q ObservedQuery) {
	o.query = q
}

func TestInterceptorRejectionDoesNotMarkHost(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	policy := &markCountingPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	cluster := testCluster(defaultProto, srv.Address)
	cluster.PoolConfig.HostSelectionPolicy = policy
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// A sent attempt marks the host, which shows the policy is in use.
	if err := db.Query("void").Exec(); err != nil {
		t.Fatal(err)
	}
	if marks := atomic.LoadInt32(&policy.marks); marks != 1 {
		t.Fatalf("expected 1 mark after a sent attempt, got %d", marks)
	}

	errRejected := errors.New("rejected")
	iter := db.Query("void").Interceptor(rejectingInterceptor{err: errRejected}).Iter()
	if err := iter.Close(); !errors.Is(err, errRejected) {
		t.Fatalf("expected %v, got %v", errRejected, err)
	}
	if marks := atomic.LoadInt32(&policy.marks); marks != 1 {
		t.Fatalf("expected a rejected attempt not to mark the host, got %d marks", marks)
	}
}

func TestInterceptorTimeExcludedFromLatency(t *testing.T) {
	srv := NewTestServer(t, defaultProto, context.Background())
	defer srv.Stop()

	db, err := newTestSession(defaultProto, srv.Address)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	interceptor := &timingInterceptor{}
	observer := &lastQueryObserver{}
	if err := db.Query("void").Interceptor(interceptor).Observer(observer).Exec(); err != nil {
		t.Fatal(err)
	}

	if observer.query.Start.Before(interceptor.preDone) {
		t.Errorf("attempt timing started before the pre-attempt interceptor returned")
	}
	if observer.query.End.After(interceptor.postStart) {
		t.Errorf("attempt timing ended after the post-attempt interceptor was called")
	}
}

func TestInterceptorChainPostAttemptWithoutChainContext(t *testing.T) {
	inner := &countingInterceptor{}
	chain := RequestInterceptorChain{Interceptors: []RequestInterceptor{inner}}

	// Neither result came from chain.InterceptPreAttempt, so there are no contexts to hand back.
	chain.InterceptPostAttempt(InterceptResult{}, nil)
	chain.InterceptPostAttempt(InterceptResult{Ctx: context.Background()}, nil)
	if inner.posts != 0 {
		t.Fatalf("expected no post-attempt calls without the chain's context, got %d", inner.posts)
	}

	res, err := chain.InterceptPreAttempt(context.Background(), RequestAttempt{})
	if err != nil {
		t.Fatal(err)
	}
	chain.InterceptPostAttempt(res, nil)
	if inner.posts != 1 {
		t.Fatalf("expected 1 post-attempt call, got %d", inner.posts)
	}
}
