gocql
=====

Package gocql implements a fast and robust Cassandra driver for the
Go programming language.

Installation
------------

    go get github.com/tux21b/gocql


Features
--------

* Modern Cassandra client for Cassandra 2.0
* Built-In support for UUIDs (version 1 and 4)


Example
-------

```go
package main

import (
	"fmt"
	"github.com/tux21b/gocql"
	"log"
)

func main() {
	// connect to your cluster
	db := gocql.NewSession(gocql.Config{
		Nodes: []string{
			"192.168.1.1",
			"192.168.1.2",
			"192.168.1.3",
		},
		Keyspace:    "example",       // (optional)
		Consistency: gocql.ConQuorum, // (optional)
	})
	defer db.Close()

	// simple query
	var title, text string
	if err := db.Query("SELECT title, text FROM posts WHERE title = ?",
		"Lorem Ipsum").Scan(&title, &text); err != nil {
		log.Fatal(err)
	}
	fmt.Println(title, text)

	// iterator example
	var titles []string
	iter := db.Query("SELECT title FROM posts").Iter()
	for iter.Scan(&title) {
		titles = append(titles, title)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(titles)

	// insertion example (with custom consistency level)
	if err := db.Query("INSERT INTO posts (title, text) VALUES (?, ?)",
		"New Title", "foobar").Consistency(gocql.ConAny).Exec(); err != nil {
		log.Fatal(err)
	}

	// prepared queries
	query := gocql.NewQuery("SELECT text FROM posts WHERE title = ?")
	if err := db.Do(query, "New Title").Scan(&text); err != nil {
		log.Fatal(err)
	}
	fmt.Println(text)
}
```

License
-------

> Copyright (c) 2012 The gocql Authors. All rights reserved.
> Use of this source code is governed by a BSD-style
> license that can be found in the LICENSE file.
