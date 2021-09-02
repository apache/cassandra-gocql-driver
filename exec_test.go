// +build all integration

package gocql

import (
	"testing"
)

func TestSingleHostQueryExecutor(t *testing.T) {
	cluster := createCluster()

	e, err := NewSingleHostQueryExecutor(cluster)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	iter := e.Iter("SELECT now() FROM system.local")

	var date []byte
	iter.Scan(&date)
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if len(date) == 0 {
		t.Fatal("expected date")
	}
}
