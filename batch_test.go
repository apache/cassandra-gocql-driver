// +build all integration

package gocql

import (
	"testing"
)

func TestBatch_Errors(t *testing.T) {
	if *flagProto == 1 {
		t.Skip("atomic batches not supported. Please use Cassandra >= 2.0")
	}

	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.batch_errors (id int primary key, val inet)`); err != nil {
		t.Fatal(err)
	}

	b := session.NewBatch(LoggedBatch)
	b.Query("SELECT * FROM batch_errors WHERE id=2 AND val=?", nil)
	if err := session.ExecuteBatch(b); err == nil {
		t.Fatal("expected to get error for invalid query in batch")
	}
}
