// +build all integration

package gocql

import (
	"strings"
	"testing"
)

func TestProto1BatchInsert(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.large (id int primary key)"); err != nil {
		t.Fatal(err)
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

func TestShouldPrepareFunction(t *testing.T) {
	var shouldPrepareTests = []struct {
		Stmt   string
		Vals   []interface{}
		Result bool
	}{
		{`
      BEGIN BATCH
        INSERT INTO users (userID, password)
        VALUES (?, ?)
      APPLY BATCH
    ;
    `, []interface{}{"smith", "secret"}, true},
		{`INSERT INTO users (userID, password, name) VALUES (?, ?, ?)`, []interface{}{"user2", "ch@ngem3b", "second user"}, true},
		{`BEGIN COUNTER BATCH UPDATE stats SET views = views + 1 WHERE pageid = ? APPLY BATCH`, []interface{}{"1"}, true},
		{`delete name from users where userID = '?';`, []interface{}{"smith"}, true},
		{`  UPDATE users SET password = '?' WHERE userID = '?'   `, []interface{}{"secret", "smith"}, true},
		{`CREATE TABLE users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint
      );`, nil, false},
		{`SELECT * FROM users WHERE user_id = 1234`, nil, false},
	}

	for _, test := range shouldPrepareTests {
		q := &Query{stmt: test.Stmt, values: test.Vals}
		if got := q.shouldPrepare(); got != test.Result {
			t.Fatalf("%q: got %v, expected %v\n", test.Stmt, got, test.Result)
		}
	}
}
