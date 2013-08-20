gocql
=====

Package gocql implements a fast and robust Cassandra client for the
Go programming language.

**Attention:** This package is currently actively developed and the API may
change in the future. The old "datbase/sql" based package is now called
[gocqldriver](https://github.com/tux21b/gocqldriver) and is no longer
maintained.

Project Website: http://tux21b.org/gocql/<br>
API documentation: http://godoc.org/tux21b.org/v1/gocql

Installation
------------

    go get tux21b.org/v1/gocql


Features
--------

* modern Cassandra client for Cassandra 1.2 and 2.0
* automatic type conversations between Cassandra and Go
  * support for all common types including sets, lists and maps
  * custom types can implement a `Marshaler` and `Unmarshaler` interface
  * strict type conversations without any loss of precision
  * built-In support for UUIDs (version 1 and 4)
* support for logged, unlogged and counter batches
* cluster management
  * automatic reconnect on connection failures with exponential falloff
  * round robin distribution of queries to different hosts
  * round robin distribution of queries to different connections on a host
  * each connection can execute up to 128 concurrent queries
* iteration over paged results with configurable page size
* optional frame compression (using snappy)
* automatic query preparation
* support for query tracing

Example
-------

```go
package main

import (
	"fmt"
	"log"

	"tux21b.org/v1/gocql"
	"tux21b.org/v1/gocql/uuid"
)

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster("192.168.1.1", "192.168.1.2", "192.168.1.3")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	session := cluster.CreateSession()
	defer session.Close()

	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", uuid.TimeUUID(), "hello world").Exec(); err != nil {
		log.Fatal(err)
	}

	var id uuid.UUID
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

License
-------

> Copyright (c) 2012 The gocql Authors. All rights reserved.
> Use of this source code is governed by a BSD-style
> license that can be found in the LICENSE file.
