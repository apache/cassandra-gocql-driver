package gocql_test

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// Example_nulls demonstrates how to distinguish between null and zero value when needed.
//
// Null values are unmarshalled as zero value of the type. If you need to distinguish for example between text
// column being null and empty string, you can unmarshal into *string field.
func Example_nulls() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.stringvals(id int, value text, PRIMARY KEY(id));
	insert into example.stringvals (id, value) values (1, null);
	insert into example.stringvals (id, value) values (2, '');
	insert into example.stringvals (id, value) values (3, 'hello');
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	scanner := session.Query(`SELECT id, value FROM stringvals`).Iter().Scanner()
	for scanner.Next() {
		var (
			id int32
			val *string
		)
		err := scanner.Scan(&id, &val)
		if err != nil {
			log.Fatal(err)
		}
		if val != nil {
			fmt.Printf("Row %d is %q\n", id, *val)
		} else {
			fmt.Printf("Row %d is null\n", id)
		}

	}
	err = scanner.Err()
	if err != nil {
		log.Fatal(err)
	}
	// Row 1 is null
	// Row 2 is ""
	// Row 3 is "hello"
}
