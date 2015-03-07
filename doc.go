// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package gocql implements a fast and robust Cassandra driver for the
Go programming language.

To get started, create a cluster definition which defines the configuration of
cluster and the session which is created. Then create a session, this session is
thread safe and is intended to be used for the entire lifecycle of the application.
Under the hood it maintains a connection pool to the Cassandra nodes.

	cluster := gocql.NewCluster(10.0.1.1, 10.0.2.1)
	cluster.Protocol = 2
	cluster.NumConns = 8
	cluster.DiscoverHosts = true

	session, err := cluster.CreateSession()
	if err != nil {
		// handle err
	}
	// use session

This will create a session using nodes '10.0.1.1' and '10.0.2.1' and then discover
the rest of the ring by polling system.peers, using a max on 8 connections per
host with protocol version 2.
*/
package gocql

// TODO(tux21b): write more docs.
