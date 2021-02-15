package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

func Example() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
	create index on example.tweet(timeline);
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	// connect to the cluster
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", gocql.TimeUUID(), "hello world").WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	var id gocql.UUID
	var text string

	/* Search for a specific set of records whose 'timeline' column matches
	 * the value 'me'. The secondary index that we created earlier will be
	 * used for optimizing the search */
	if err := session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`,
		"me").WithContext(ctx).Consistency(gocql.One).Scan(&id, &text); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tweet:", id, text)
	fmt.Println()

	// list all tweets
	scanner := session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`,
		"me").WithContext(ctx).Iter().Scanner()
	for scanner.Next() {
		err = scanner.Scan(&id, &text)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Tweet:", id, text)
	}
	// scanner.Err() closes the iterator, so scanner nor iter should be used afterwards.
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	// Tweet: cad53821-3731-11eb-971c-708bcdaada84 hello world
	//
	// Tweet: cad53821-3731-11eb-971c-708bcdaada84 hello world
	// Tweet: d577ab85-3731-11eb-81eb-708bcdaada84 hello world
}
