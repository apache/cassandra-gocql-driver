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
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type MyRequestInterceptor struct {
	injectFault bool
	contextStr  string
	t           *testing.T
}

type traceIDKey struct{}

var _ gocql.RequestInterceptor = (*MyRequestInterceptor)(nil)

func (q MyRequestInterceptor) InterceptPreAttempt(
	ctx context.Context,
	attempt gocql.ExecAttempt,
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
		return gocql.InterceptResult{}, fmt.Errorf("timeout")
	}

	return gocql.InterceptResult{Ctx: ctx}, nil
}

func (q MyRequestInterceptor) InterceptPostAttempt(res gocql.InterceptResult, _ error) {
	require.Equal(q.t, q.contextStr, res.Ctx.Value(traceIDKey{}))
}

// Demonstrates how a RequestInterceptor might be used to inject faults.
func TestExampleInterceptorFault(t *testing.T) {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.RequestInterceptor = MyRequestInterceptor{injectFault: true, t: t}

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
	require.Equal(t, err, fmt.Errorf("timeout"))
}

// Demonstrates how a RequestInterceptor might be used to propagate ancillary data to post-processing via the context.
func TestExampleInterceptorContext(t *testing.T) {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.RequestInterceptor = MyRequestInterceptor{injectFault: false, t: t}

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
	require.NoError(t, err)
}

// Demonstrates how to use a request interceptor chain.
func TestInterceptorChain(t *testing.T) {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.RequestInterceptor = gocql.RequestInterceptorChain{
		[]gocql.RequestInterceptor{
			MyRequestInterceptor{t: t, contextStr: "123"},
			MyRequestInterceptor{t: t, contextStr: "234"},
			MyRequestInterceptor{t: t, contextStr: "345"},
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
	require.NoError(t, err)
}
