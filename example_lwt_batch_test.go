package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// ExampleSession_MapExecuteBatchCAS demonstrates how to execute a batch lightweight transaction.
func ExampleSession_MapExecuteBatchCAS() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.my_lwt_batch_table(pk text, ck text, version int, value text, PRIMARY KEY(pk, ck));
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

	err = session.Query("INSERT INTO example.my_lwt_batch_table (pk, ck, version, value) VALUES (?, ?, ?, ?)",
		"pk1", "ck1", 1, "a").WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = session.Query("INSERT INTO example.my_lwt_batch_table (pk, ck, version, value) VALUES (?, ?, ?, ?)",
		"pk1", "ck2", 1, "A").WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}

	executeBatch := func(ck2Version int) {
		b := session.NewBatch(gocql.LoggedBatch)
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
			Args:       []interface{}{"b", "pk1", "ck1", 1},
		})
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
			Args:       []interface{}{"B", "pk1", "ck2", ck2Version},
		})
		m := make(map[string]interface{})
		applied, iter, err := session.MapExecuteBatchCAS(b.WithContext(ctx), m)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(applied, m)

		m = make(map[string]interface{})
		for iter.MapScan(m) {
			fmt.Println(m)
			m = make(map[string]interface{})
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}
	}

	printState := func() {
		scanner := session.Query("SELECT ck, value FROM example.my_lwt_batch_table WHERE pk = ?", "pk1").
			WithContext(ctx).Iter().Scanner()
		for scanner.Next() {
			var ck, value string
			err = scanner.Scan(&ck, &value)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(ck, value)
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}

	executeBatch(0)
	printState()
	executeBatch(1)
	printState()

	// false map[ck:ck1 pk:pk1 version:1]
	// map[[applied]:false ck:ck2 pk:pk1 version:1]
	// ck1 a
	// ck2 A
	// true map[]
	// ck1 b
	// ck2 B
}
