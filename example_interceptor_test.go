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

package gocql_test

import (
	"context"
	"errors"
	"log"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type MyRequestInterceptor struct {
	injectFault bool
	contextStr  string
}

type traceIDKey struct{}

var errInjectedTimeout = errors.New("timeout")

var _ gocql.RequestInterceptor = (*MyRequestInterceptor)(nil)

func (q MyRequestInterceptor) InterceptPreAttempt(
	ctx context.Context,
	attempt gocql.RequestAttempt,
) (gocql.InterceptResult, error) {
	switch attempt.Type {
	case gocql.StatementQuery:
		// Inspect query
		log.Println(attempt.Query.Statement())
	case gocql.StatementBatch:
		// Inspect batch
		log.Println(attempt.Batch.Entries[0].Stmt)
	}

	// Inspect or modify context
	ctx = context.WithValue(ctx, traceIDKey{}, q.contextStr)

	// Optionally bypass request issuance by returning an error to prevent query execution.
	// For example, to simulate query timeouts, or to perform client-side load shedding.
	if q.injectFault {
		return gocql.InterceptResult{}, errInjectedTimeout
	}

	return gocql.InterceptResult{Ctx: ctx}, nil
}

func (q MyRequestInterceptor) InterceptPostAttempt(res gocql.InterceptResult, _ error) {
	if res.Ctx == nil {
		return
	}
	if q.contextStr != res.Ctx.Value(traceIDKey{}) {
		log.Printf("context string changed: %s", res.Ctx.Value(traceIDKey{}))
	}
}

// Demonstrates how a RequestInterceptor might be used to inject faults.
func Example_interceptorFault() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.RequestInterceptor = MyRequestInterceptor{injectFault: true}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	var stringValue string
	// Here we specify a retry policy, but the GoCQL library with SimpleRetryPolicy will only retry multiple
	// times if you have more than one host. This may not be the case if you are running this example against
	// a local test Cassandra instance, so NumRetries: 2 is here only for explanatory purposes.
	err = session.Query("select now() from system.local").
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 2}).
		Idempotent(true).
		ScanContext(ctx, &stringValue)
	if !errors.Is(err, errInjectedTimeout) {
		log.Fatal(err)
	}
}

// Demonstrates how a RequestInterceptor might be used to propagate ancillary data to post-processing via the context.
func Example_interceptorContext() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.RequestInterceptor = MyRequestInterceptor{injectFault: false}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	var stringValue string
	err = session.Query("select now() from system.local").
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 2}).
		ScanContext(ctx, &stringValue)
	if err != nil {
		log.Fatal(err)
	}
}

// Demonstrates how to use a request interceptor chain.
func Example_interceptorChain() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.RequestInterceptor = gocql.RequestInterceptorChain{
		[]gocql.RequestInterceptor{
			MyRequestInterceptor{contextStr: "123"},
			MyRequestInterceptor{contextStr: "234"},
			MyRequestInterceptor{contextStr: "345"},
		},
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	var stringValue string
	err = session.Query("select now() from system.local").
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 2}).
		ScanContext(ctx, &stringValue)
	if err != nil {
		log.Fatal(err)
	}
}
