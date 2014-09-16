// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//This file will be the future home for more policies
package gocql

//RetryableQuery is an interface that represents a query or batch statement that
//exposes the correct functions for the retry policy logic to evaluate correctly.
type RetryableQuery interface {
	Attempts() int
}

// RetryPolicy is the interface that is used by gocql to determine if a query
// can be retried based on the policy provided by the using application.
// See SimpleRetryPolicy for an example of how to create a custom policy.
type RetryPolicy interface {
	Attempt(RetryableQuery) bool
}

// SimpleRetryPolicy is just a simple number of retries logic.
type SimpleRetryPolicy struct {
	NumRetries int //Number of times to retry a query
}

//Attempt tells gocql to attempt the query again.
func (s *SimpleRetryPolicy) Attempt(q RetryableQuery) bool {
	return q.Attempts() <= s.NumRetries
}
