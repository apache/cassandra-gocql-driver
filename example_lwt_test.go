package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// ExampleQuery_MapScanCAS demonstrates how to execute a single-statement lightweight transaction.
func ExampleQuery_MapScanCAS() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.my_lwt_table(pk int, version int, value text, PRIMARY KEY(pk));
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	err = session.Query("INSERT INTO example.my_lwt_table (pk, version, value) VALUES (?, ?, ?)",
		1, 1, "a").WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}
	m := make(map[string]interface{})
	applied, err := session.Query("UPDATE example.my_lwt_table SET value = ? WHERE pk = ? IF version = ?",
		"b", 1, 0).WithContext(ctx).MapScanCAS(m)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(applied, m)

	var value string
	err = session.Query("SELECT value FROM example.my_lwt_table WHERE pk = ?", 1).WithContext(ctx).
		Scan(&value)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)

	m = make(map[string]interface{})
	applied, err = session.Query("UPDATE example.my_lwt_table SET value = ? WHERE pk = ? IF version = ?",
		"b", 1, 1).WithContext(ctx).MapScanCAS(m)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(applied, m)

	var value2 string
	err = session.Query("SELECT value FROM example.my_lwt_table WHERE pk = ?", 1).WithContext(ctx).
		Scan(&value2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value2)
	// false map[version:1]
	// a
	// true map[]
	// b
}
