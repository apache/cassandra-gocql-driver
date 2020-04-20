// +build all integration
// +build !scylla

package gocql

import "testing"

func TestUDF(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < 4 {
		t.Skip("skipping UDF support on proto < 4")
	}

	const query = `CREATE OR REPLACE FUNCTION uniq(state set<text>, val text)
	  CALLED ON NULL INPUT RETURNS set<text> LANGUAGE java
	  AS 'state.add(val); return state;'`

	err := session.Query(query).Exec()
	if err != nil {
		t.Fatal(err)
	}
}
