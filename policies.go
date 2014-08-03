// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//This file will be the future home for more policies
package gocql

// RetryPolicy represents the retry behavour for a query.
type RetryPolicy struct {
	NumRetries int //Number of times to retry a query
}
