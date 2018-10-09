// +build all integration_env

package gocql

// This file groups integration tests where Cassandra has to be set up with some special integration variables
import (
	"reflect"
	"testing"
)

func TestWriteFailure(t *testing.T) {
	cluster := createCluster()
	createKeyspace(t, cluster, "test")
	cluster.Keyspace = "test"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("create session:", err)
	}
	defer session.Close()

	if err := createTable(session, "CREATE TABLE test.test (id int,value int,PRIMARY KEY (id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	if err := session.Query(`INSERT INTO test.test (id, value) VALUES (1, 1)`).Exec(); err != nil {
		errWrite, ok := err.(*RequestErrWriteFailure)
		if ok {
			if session.cfg.ProtoVersion >= 5 {
				// ErrorMap should be filled with some hosts that should've errored
				if len(errWrite.ErrorMap) == 0 {
					t.Fatal("errWrite.ErrorMap should have some failed hosts but it didn't have any")
				}
			} else {
				// Map doesn't get filled for V4
				if len(errWrite.ErrorMap) != 0 {
					t.Fatal("errWrite.ErrorMap should have length 0, it's: ", len(errWrite.ErrorMap))
				}
			}
		} else {
			t.Fatal("error should be RequestErrWriteFailure, it's: ", errWrite)
		}
	} else {
		t.Fatal("a write fail error should have happened when querying test keyspace")
	}

	if err = session.Query("DROP KEYSPACE test").Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomPayloadMessages(t *testing.T) {
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testCustomPayloadMessages (id int, value int, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	// QueryMessage
	var customPayload = map[string][]byte{"a": []byte{10, 20}, "b": []byte{20, 30}}
	query := session.Query("SELECT id FROM testCustomPayloadMessages where id = ?", 42).Consistency(One).CustomPayload(customPayload)
	iter := query.Iter()
	rCustomPayload := iter.GetCustomPayload()
	if !reflect.DeepEqual(customPayload, rCustomPayload) {
		t.Fatal("The received custom payload should match the sent")
	}
	iter.Close()

	// Insert query
	query = session.Query("INSERT INTO testCustomPayloadMessages(id,value) VALUES(1, 1)").Consistency(One).CustomPayload(customPayload)
	iter = query.Iter()
	rCustomPayload = iter.GetCustomPayload()
	if !reflect.DeepEqual(customPayload, rCustomPayload) {
		t.Fatal("The received custom payload should match the sent")
	}
	iter.Close()

	// Batch Message
	b := session.NewBatch(LoggedBatch)
	b.CustomPayload = customPayload
	b.Query("INSERT INTO testCustomPayloadMessages(id,value) VALUES(1, 1)")
	if err := session.ExecuteBatch(b); err != nil {
		t.Fatalf("query failed. %v", err)
	}
}
