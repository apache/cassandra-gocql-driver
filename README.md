gocql
=====

[![Build Status](https://travis-ci.org/tux21b/gocql.png?branch=master)](https://travis-ci.org/tux21b/gocql)
[![GoDoc](http://godoc.org/tux21b.org/v1/gocql?status.png)](http://godoc.org/tux21b.org/v1/gocql)

**Package Status:** Alpha 

Package gocql implements a fast and robust Cassandra client for the
Go programming language.

Project Website: http://tux21b.org/gocql/<br>
API documentation: http://godoc.org/tux21b.org/v1/gocql<br>
Discussions: https://groups.google.com/forum/#!forum/gocql

Installation
------------

    go get tux21b.org/v1/gocql


Features
--------

* Modern Cassandra client for Cassandra 1.2 and 2.0
* Automatic type conversations between Cassandra and Go
  * Support for all common types including sets, lists and maps
  * Custom types can implement a `Marshaler` and `Unmarshaler` interface
  * Strict type conversations without any loss of precision
  * Built-In support for UUIDs (version 1 and 4)
* Support for logged, unlogged and counter batches
* Cluster management
  * Automatic reconnect on connection failures with exponential falloff
  * Round robin distribution of queries to different hosts
  * Round robin distribution of queries to different connections on a host
  * Each connection can execute up to 128 concurrent queries
* Iteration over paged results with configurable page size
* Optional frame compression (using snappy)
* Automatic query preparation
* Support for query tracing

Please visit the [Roadmap](https://github.com/tux21b/gocql/wiki/Roadmap) page to see what is on the horizion.

Example
-------

```go
package main

import (
	"fmt"
	"log"

	"tux21b.org/v1/gocql"
)

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster("192.168.1.1", "192.168.1.2", "192.168.1.3")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", gocql.TimeUUID(), "hello world").Exec(); err != nil {
		log.Fatal(err)
	}

	var id gocql.UUID
	var text string

	// select a single tweet
	if err := session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`,
		"me").Consistency(gocql.One).Scan(&id, &text); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tweet:", id, text)

	// list all tweets
	iter := session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`, "me").Iter()
	for iter.Scan(&id, &text) {
		fmt.Println("Tweet:", id, text)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
```

Other Projects
--------------

* [cqlc](http://relops.com/cqlc) generates gocql compliant code from your Cassandra schema so that you can write type safe CQL statements in Go with a natural query syntax.
* [gocqldriver](https://github.com/tux21b/gocqldriver) is the predecessor of gocql based on Go's "database/sql" package. This project isn't maintained anymore, because Cassandra wasn't a good fit for the traditional "database/sql" API. Use this package instead.

License
-------

> Copyright (c) 2012-2014 The gocql Authors. All rights reserved.
> Use of this source code is governed by a BSD-style
> license that can be found in the LICENSE file.
