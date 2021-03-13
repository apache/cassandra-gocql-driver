package gocql_test

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"sort"
)

// Example_set demonstrates how to use sets.
func Example_set() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.sets(id int, value set<text>, PRIMARY KEY(id));
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	err = session.Query(`UPDATE sets SET value=? WHERE id=1`, []string{"alpha", "beta", "gamma"}).Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = session.Query(`UPDATE sets SET value=value+? WHERE id=1`, "epsilon").Exec()
	if err != nil {
		// This does not work because the ? expects a set, not a single item.
		fmt.Printf("expected error: %v\n", err)
	}
	err = session.Query(`UPDATE sets SET value=value+? WHERE id=1`, []string{"delta"}).Exec()
	if err != nil {
		log.Fatal(err)
	}
	// map[x]struct{} is supported too.
	toRemove := map[string]struct{}{
		"alpha": {},
		"gamma": {},
	}
	err = session.Query(`UPDATE sets SET value=value-? WHERE id=1`, toRemove).Exec()
	if err != nil {
		log.Fatal(err)
	}
	scanner := session.Query(`SELECT id, value FROM sets`).Iter().Scanner()
	for scanner.Next() {
		var (
			id int32
			val []string
		)
		err := scanner.Scan(&id, &val)
		if err != nil {
			log.Fatal(err)
		}
		sort.Strings(val)
		fmt.Printf("Row %d is %v\n", id, val)
	}
	err = scanner.Err()
	if err != nil {
		log.Fatal(err)
	}
	// expected error: can not marshal string into set(varchar)
	// Row 1 is [beta delta]
}
