package gocql

import (
	"testing"
)

// FuzzMarshalFloat64Ptr aimed to repeatedly test float64 marshaling with generated inputs based on seed corpus.
func FuzzMarshalFloat64Ptr(f *testing.F) {
	f.Add(float64(7500), float64(7500.00))

	f.Fuzz(func(t *testing.T, num, numWithPoints float64) {
		session := createSession(t)
		defer session.Close()

		if err := createTable(session, "CREATE TABLE IF NOT EXISTS gocql_test.float_test (id double, test double, primary key (id))"); err != nil {
			t.Fatal("create table:", err)
		}

		if err := session.Query(`TRUNCATE TABLE gocql_test.float_test`).Exec(); err != nil {
			t.Fatal("truncate table:", err)
		}

		if err := session.Query(`INSERT INTO float_test (id,test) VALUES (?,?)`, numWithPoints, &num).Exec(); err != nil {
			t.Fatal("insert float64:", err)
		}
	})
}
