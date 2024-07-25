package gocql

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestDisableHostLookup(t *testing.T) {
	cluster := createCluster()
	cluster.DisableHostLookup = true

	cluster.NumConns = 1
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE IF NOT EXISTS gocql_test.test_table (id int primary key)"); err != nil {
		t.Fatal(err)
	}
	if err := session.Query("insert into gocql_test.test_table (id) values (?)", 123).Exec(); err != nil {
		t.Error(err)
		return
	}

	go simulateHeartbeatFailure(session)

	queryCycles := 20
	for ; queryCycles > 0; queryCycles-- {
		time.Sleep(2 * time.Second)
		queryTestKeyspace(session)
		triggerSchemaChange(session)
	}
}

func queryTestKeyspace(session *Session) {
	iter := session.Query("SELECT * FROM gocql_test.test_table").Iter()

	var id string
	for iter.Scan(&id) {
		fmt.Printf("id: %s\n", id)
	}
	if err := iter.Close(); err != nil {
		log.Printf("error querying: %v", err)
	}
}

func triggerSchemaChange(session *Session) {
	// Create a keyspace to trigger schema agreement
	err := session.Query(`CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}`).Exec()
	if err != nil {
		log.Printf("unable to create keyspace: %v", err)
	}

	// Alter the keyspace to trigger schema agreement
	err = session.Query(`ALTER KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}`).Exec()
	if err != nil {
		log.Printf("unable to alter keyspace: %v", err)
	}

	// Drop the keyspace after schema agreement
	err = session.Query(`DROP KEYSPACE IF EXISTS test_keyspace`).Exec()
	if err != nil {
		log.Printf("unable to drop keyspace: %v", err)
	}
}

func simulateHeartbeatFailure(session *Session) {
	time.Sleep(20 * time.Second) // simulate 20 seconds of normal operation
	log.Println("Simulating heartbeat failure...")
	cluster := session.cfg
	session.Close() // close the session to simulate a heartbeat failure

	time.Sleep(10 * time.Second) // wait for 10 seconds to simulate downtime

	// Reconnect
	log.Println("Reconnecting...")
	newSession, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("unable to create session: %v", err)
	}
	*session = *newSession
	log.Println("Reconnected")
}
