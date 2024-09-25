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
	"log"
	"time"

	gocql "github.com/gocql/gocql"
)

type MyQueryAttemptInterceptor struct {
	injectFault bool
}

func (q MyQueryAttemptInterceptor) Intercept(
	ctx context.Context,
	attempt gocql.QueryAttempt,
	handler gocql.QueryAttemptHandler,
) (*gocql.Iter, error) {
	switch q := attempt.Query.(type) {
	case *gocql.Query:
		// Inspect or modify query
		attempt.Query = q
	case *gocql.Batch:
		// Inspect or modify batch
		attempt.Query = q
	}

	// Inspect or modify context
	ctx = context.WithValue(ctx, "trace-id", "123")

	// Optionally bypass the handler and return an error to prevent query execution.
	// For example, to simulate query timeouts.
	if q.injectFault && attempt.Attempts == 0 {
		<-time.After(1 * time.Second)
		return nil, gocql.RequestErrWriteTimeout{}
	}

	// The interceptor *must* invoke the handler to execute the query.
	return handler(ctx, attempt), nil
}

// Example_interceptor demonstrates how to implement a QueryAttemptInterceptor.
func Example_interceptor() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.QueryAttemptInterceptor = MyQueryAttemptInterceptor{injectFault: true}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	var stringValue string
	err = session.Query("select now() from system.local").
		WithContext(ctx).
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 2}).
		Scan(&stringValue)
	if err != nil {
		log.Fatalf("query failed %T", err)
	}
}
