gocql
=====

The gocql package provides a database/sql driver for CQL, the Cassandra
query language.

This package requires a recent version of Cassandra (≥ 1.2) that supports
CQL 3.0 and the new native protocol. The native protocol is still considered
beta and must be enabled manually in Cassandra 1.2 by setting
"start_native_transport" to true in conf/cassandra.yaml.

**Note:** gocql requires the tip version of Go, as some changes in the 
`database/sql` have not made it into 1.0.x yet. There is 
[a fork](https://github.com/titanous/gocql) that backports these changes 
to Go 1.0.3.

Installation
------------

    go get github.com/tux21b/gocql

Example
-------

```go
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/tux21b/gocql"
)

func main() {
	db, err := sql.Open("gocql", "localhost:9042 keyspace=system")
	if err != nil {
		fmt.Println("Open error:", err)
	}

	rows, err := db.Query("SELECT keyspace_name FROM schema_keyspaces")
	if err != nil {
		fmt.Println("Query error:", err)
	}

	for rows.Next() {
		var keyspace string
		err = rows.Scan(&keyspace)
		if err != nil {
			fmt.Println("Scan error:", err)
		}
		fmt.Println(keyspace)
	}

	if err = rows.Err(); err != nil {
		fmt.Println("Iteration error:", err)
		return
	}
}
```

Please see `gocql_test.go` for some more advanced examples.

Features
--------

* Modern Cassandra client that is based on Cassandra's new native protocol
* Compatible with Go's `database/sql` package
* Built-In support for UUIDs (version 1 and 4)
* Optional frame compression (using snappy)

License
-------

> Copyright (c) 2012 The gocql Authors. All rights reserved.
> Use of this source code is governed by a BSD-style
> license that can be found in the LICENSE file.
