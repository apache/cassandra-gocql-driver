// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

//RetyPolicy policy defines the rety stradegy for a query. It can be assigned to the
//cluster configuration or per query. Set a property to 0 for no retries to happen at
//that level. Setting all properties to 0 will prevent a query from being retried up
// error.
type RetryPolicy struct {
	Host       int
	Rack       int
	DataCenter int
	counter    []int
}
