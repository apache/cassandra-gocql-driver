package gocql

import (
	"strings"
	"testing"
)

func TestProto1BatchInsert(t *testing.T) {
	session := createSession(t)
	if err := session.Query("CREATE TABLE large (id int primary key)").Exec(); err != nil {
		t.Fatal("create table:", err)
	}

	begin := "BEGIN BATCH"
	end := "APPLY BATCH"
	query := "INSERT INTO large (id) VALUES (?)"
	fullQuery := strings.Join([]string{begin, query, end}, "\n")
	args := []interface{}{5}
	if err := session.Query(fullQuery, args...).Consistency(Quorum).Exec(); err != nil {
		t.Fatal(err)
	}

}
