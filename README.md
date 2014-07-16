gocql
=====

[![Build Status](https://travis-ci.org/gocql/gocql.png?branch=master)](https://travis-ci.org/gocql/gocql)
[![GoDoc](http://godoc.org/github.com/gocql/gocql?status.png)](http://godoc.org/github.com/gocql/gocql)

**Package Status:** Alpha 

Package gocql implements a fast and robust Cassandra client for the
Go programming language.

Project Website: http://gocql.github.io/<br>
API documentation: http://godoc.org/github.com/gocql/gocql<br>
Discussions: https://groups.google.com/forum/#!forum/gocql

Supported Versions: Cassandra 1.2, 2.0, 2.1; Go 1.2, 1.3

Installation
------------

    go get github.com/gocql/gocql


Features
--------

* Modern Cassandra client using the native transport
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
  * Optional automatic discovery of nodes
* Iteration over paged results with configurable page size
* Optional frame compression (using snappy)
* Automatic query preparation
* Support for query tracing

Please visit the [Roadmap](https://github.com/gocql/gocql/wiki/Roadmap) page to see what is on the horizion.

Important Default Keyspace Changes
----------------------------------
gocql no longer supports executing "use <keyspace>" statements to simplfy the library. The user still has the
ability to define the default keyspace for connections but now the keyspace can only be defined before a
session is created. Queries can still access keyspaces by indicating the keyspace in the query:
```sql
SELECT * FROM example2.table;
```

Example of correct usage:
```go
	cluster := gocql.NewCluster("192.168.1.1", "192.168.1.2", "192.168.1.3")
	cluster.Keyspace = "example"
	...
	session, err := cluster.CreateSession()

```
Example of incorrect usage:
```go
	cluster := gocql.NewCluster("192.168.1.1", "192.168.1.2", "192.168.1.3")
	cluster.Keyspace = "example"
	...
	session, err := cluster.CreateSession()

	if err = session.Query("use example2").Exec(); err != nil {
		log.Fatal(err)
	}
```
This will result in an err being returned from the session.Query line as the user is trying to execute a "use"
statement. 

Example
-------

```go
package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
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
