// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//This file will be the future home for more policies
package gocql

//RetryableQuery is an interface that represents a query or batch statement that
//exposes the correct functions for the retry policy logic to evaluate correctly.
type RetryableQuery interface {
	Attempts() int
	GetConsistency() Consistency
}

// RetryPolicy interace is used by gocql to determine if a query can be attempted
// again after a retryable error has been received. The interface allows gocql
// users to implement their own logic to determine if a query can be attempted
// again.
//
// See SimpleRetryPolicy as an example of implementing and using a RetryPolicy
// interface.
type RetryPolicy interface {
	Attempt(RetryableQuery) bool
}

/*
SimpleRetryPolicy has simple logic for attempting a query a fixed number of times.

See below for examples of usage:

	//Assign to the cluster
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	//Assign to a query
 	query.RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 1})
*/
type SimpleRetryPolicy struct {
	NumRetries int //Number of times to retry a query
}

// Attempt tells gocql to attempt the query again based on query.Attempts being less
// than the NumRetries defined in the policy.
func (s *SimpleRetryPolicy) Attempt(q RetryableQuery) bool {
	return q.Attempts() <= s.NumRetries
}
