gocql
=====

The gocql package provides a database/sql driver for CQL, the Cassandra
query language.

This package requires a recent version of Cassandra (≥ 1.2) that supports
CQL 3.0 and the new native protocol. The native protocol is still considered
beta and must be enabled manually in Cassandra 1.2 by setting
"start_native_transport" to true in conf/cassandra.yaml.

Installation
------------

    go get github.com/tux21b/gocql

Example
-------

    db, err := sql.Open("gocql", "localhost:8000 keyspace=system")
    // ...
    rows, err := db.Query("SELECT keyspace_name FROM schema_keyspaces")
    // ...
    for rows.Next() {
         var keyspace string
         err = rows.Scan(&keyspace)
         // ...
         fmt.Println(keyspace)
    }
    if err := rows.Err(); err != nil {
        // ...
    }

Features
--------

* Modern Cassandra client that is based on Cassandra's new native protocol
* Compatible with Go's `database/sql` package
* Built-In support for UUIDs (version 1 and 4)

License
-------

> Copyright (c) 2012 by Christoph Hack <christoph@tux21b.org>  
> All rights reserved. Distributed under the Simplified BSD License.
