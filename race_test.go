// +build all integration

package gocql

import (
	"fmt"
	"testing"
	"time"
	"github.com/stretchr/testify/require"
)

func getCassandraSession() *Session {

	cluster := NewCluster("127.0.0.1")
	cluster.Port = 9042
	cluster.Timeout = 40 * time.Second
	cluster.ConnectTimeout = 40 * time.Second
	cluster.NumConns = 100

	dbSession, err := cluster.CreateSession()
	if err != nil {
		fmt.Errorf("failed to get cassandra session: %v", err)
		return nil
	}
	return dbSession
}

func TestGocqlMViewDataRaceOriginal(t *testing.T) {
	// keyspace := `keyspace`
	// table := `table`
	// view := `view`
	id1 := `c818ce88-78b5-46a3-8370-18fdbc330828`

	dropKeyspaceStmt := `DROP KEYSPACE IF EXISTS "keyspace"`
	createKeyspaceStmt := `CREATE KEYSPACE IF NOT EXISTS "keyspace" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }`

	createTableStmt := `CREATE TABLE IF NOT EXISTS "keyspace"."table" ("ID" uuid, "Name" varchar, PRIMARY KEY (("ID")));`

	insertStmt := `INSERT INTO "keyspace"."table" ("ID", "Name") VALUES (?, ?)`

	s := getCassandraSession()
	require.NotNil(t, s)

	err := s.Query(dropKeyspaceStmt).Exec()
	require.NoError(t, err)

	err = s.Query(createKeyspaceStmt).Exec()
	require.NoError(t, err)

	err = s.Query(createTableStmt).Exec()
	require.NoError(t, err)

	// increase the number of iterations OR run the test multiple times.
	for i := 0; i < 100000; i++ {
		err = s.Query(insertStmt, id1, "name").Exec()
	}
}

func TestGocqlMViewDataRaceSession(t *testing.T) {
	const table = "TestGocqlMViewDataRace"
	const id1 = "c818ce88-78b5-46a3-8370-18fdbc330828"

	for index := 0; index < 1000; index++ {
			s := createSession(t)
			s.Close()
		}
	fmt.Println("Iter")
}

