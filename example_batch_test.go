package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// Example_batch demonstrates how to execute a batch of statements.
func Example_batch() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.batches(pk int, ck int, description text, PRIMARY KEY(pk, ck));
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

	b := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       "INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		Args:       []interface{}{1, 2, "1.2"},
		Idempotent: true,
	})
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       "INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		Args:       []interface{}{1, 3, "1.3"},
		Idempotent: true,
	})
	err = session.ExecuteBatch(b)
	if err != nil {
		log.Fatal(err)
	}

	scanner := session.Query("SELECT pk, ck, description FROM example.batches").Iter().Scanner()
	for scanner.Next() {
		var pk, ck int32
		var description string
		err = scanner.Scan(&pk, &ck, &description)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(pk, ck, description)
	}
	// 1 2 1.2
	// 1 3 1.3
}
