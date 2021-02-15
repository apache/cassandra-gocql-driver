package gocql_test

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// Example_paging demonstrates how to manually fetch pages and use page state.
//
// See also package documentation about paging.
func Example_paging() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.itoa(id int, description text, PRIMARY KEY(id));
	insert into example.itoa (id, description) values (1, 'one');
	insert into example.itoa (id, description) values (2, 'two');
	insert into example.itoa (id, description) values (3, 'three');
	insert into example.itoa (id, description) values (4, 'four');
	insert into example.itoa (id, description) values (5, 'five');
	insert into example.itoa (id, description) values (6, 'six');
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	var pageState []byte
	for {
		// We use PageSize(2) for the sake of example, use larger values in production (default is 5000) for performance
		// reasons.
		iter := session.Query(`SELECT id, description FROM itoa`).PageSize(2).PageState(pageState).Iter()
		nextPageState := iter.PageState()
		scanner := iter.Scanner()
		for scanner.Next() {
			var (
				id int
				description string
			)
			err = scanner.Scan(&id, &description)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(id, description)
		}
		err = scanner.Err()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("next page state: %+v\n", nextPageState)
		if len(nextPageState) == 0 {
			break
		}
		pageState = nextPageState
	}
	// 5 five
	// 1 one
	// next page state: [4 0 0 0 1 0 240 127 255 255 253 0]
	// 2 two
	// 4 four
	// next page state: [4 0 0 0 4 0 240 127 255 255 251 0]
	// 6 six
	// 3 three
	// next page state: [4 0 0 0 3 0 240 127 255 255 249 0]
	// next page state: []
}
