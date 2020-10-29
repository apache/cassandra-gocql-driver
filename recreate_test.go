//+build integration
//+build scylla

// Copyright (C) 2017 ScyllaDB

package gocql

import (
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRecreateSchema(t *testing.T) {
	session := createSessionFromCluster(createCluster(), t)
	defer session.Close()

	tcs := []struct {
		Name     string
		Keyspace string
		Input    string
		Golden   string
	}{
		{
			Name:     "Keyspace",
			Keyspace: "gocqlx_keyspace",
			Input:    "testdata/recreate/keyspace.cql",
			Golden:   "testdata/recreate/keyspace_golden.cql",
		},
		{
			Name:     "Table",
			Keyspace: "gocqlx_table",
			Input:    "testdata/recreate/table.cql",
			Golden:   "testdata/recreate/table_golden.cql",
		},
		{
			Name:     "Materialized Views",
			Keyspace: "gocqlx_mv",
			Input:    "testdata/recreate/materialized_views.cql",
			Golden:   "testdata/recreate/materialized_views_golden.cql",
		},
		{
			Name:     "Index",
			Keyspace: "gocqlx_idx",
			Input:    "testdata/recreate/index.cql",
			Golden:   "testdata/recreate/index_golden.cql",
		},
		{
			Name:     "Secondary Index",
			Keyspace: "gocqlx_sec_idx",
			Input:    "testdata/recreate/secondary_index.cql",
			Golden:   "testdata/recreate/secondary_index_golden.cql",
		},
		{
			Name:     "UDT",
			Keyspace: "gocqlx_udt",
			Input:    "testdata/recreate/udt.cql",
			Golden:   "testdata/recreate/udt_golden.cql",
		},
	}

	for i := range tcs {
		test := tcs[i]
		t.Run(test.Name, func(t *testing.T) {
			cleanup(t, session, test.Keyspace)

			in, err := ioutil.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}

			queries := trimQueries(strings.Split(string(in), ";"))
			for _, q := range queries {
				qr := session.Query(q, nil)
				if err := qr.Exec(); err != nil {
					t.Fatal("invalid input query", q, err)
				}
				qr.Release()
			}

			km, err := session.KeyspaceMetadata(test.Keyspace)
			if err != nil {
				t.Fatal("dump schema", err)
			}
			dump, err := km.ToCQL()
			if err != nil {
				t.Fatal("recreate schema", err)
			}

			golden, err := ioutil.ReadFile(test.Golden)
			if err != nil {
				t.Fatal(err)
			}

			goldenQueries := sortQueries(strings.Split(string(golden), ";"))
			dumpQueries := sortQueries(strings.Split(dump, ";"))

			if len(goldenQueries) != len(dumpQueries) {
				t.Errorf("Expected len(dumpQueries) to be %d, got %d", len(goldenQueries), len(dumpQueries))
			}
			// Compare with golden
			for i, dq := range dumpQueries {
				gq := goldenQueries[i]

				if diff := cmp.Diff(gq, dq); diff != "" {
					t.Errorf("dumpQueries[%d] diff\n%s", i, diff)
				}
			}

			// Exec dumped queries to check if they are CQL-correct
			cleanup(t, session, test.Keyspace)
			session.schemaDescriber.clearSchema(test.Keyspace)

			for _, q := range trimQueries(strings.Split(dump, ";")) {
				qr := session.Query(q, nil)
				if err := qr.Exec(); err != nil {
					t.Fatal("invalid dump query", q, err)
				}
				qr.Release()
			}

			// Check if new dump is the same as previous
			km, err = session.KeyspaceMetadata(test.Keyspace)
			if err != nil {
				t.Fatal("dump schema", err)
			}
			secondDump, err := km.ToCQL()
			if err != nil {
				t.Fatal("recreate schema", err)
			}

			secondDumpQueries := sortQueries(strings.Split(secondDump, ";"))

			if !cmp.Equal(secondDumpQueries, dumpQueries) {
				t.Errorf("first dump and second one differs: %s", cmp.Diff(secondDumpQueries, dumpQueries))
			}
		})
	}
}

func TestScyllaEncryptionOptionsUnmarshaller(t *testing.T) {
	const (
		input  = "testdata/recreate/scylla_encryption_options.bin"
		golden = "testdata/recreate/scylla_encryption_options_golden.json"
	)

	inputBuf, err := ioutil.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}

	goldenBuf, err := ioutil.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}

	goldenOpts := &scyllaEncryptionOptions{}
	if err := json.Unmarshal(goldenBuf, goldenOpts); err != nil {
		t.Fatal(err)
	}

	opts := &scyllaEncryptionOptions{}
	if err := opts.UnmarshalBinary(inputBuf); err != nil {
		t.Error(err)
	}

	if !cmp.Equal(goldenOpts, opts) {
		t.Error(cmp.Diff(goldenOpts, opts))
	}

}

func cleanup(t *testing.T, session *Session, keyspace string) {
	qr := session.Query(`DROP KEYSPACE IF EXISTS ` + keyspace)
	if err := qr.Exec(); err != nil {
		t.Fatalf("unable to drop keyspace: %v", err)
	}
	qr.Release()
}

func sortQueries(in []string) []string {
	q := trimQueries(in)
	sort.Strings(q)
	return q
}

func trimQueries(in []string) []string {
	queries := in[:0]
	for _, q := range in {
		q = strings.TrimSpace(q)
		if len(q) != 0 {
			queries = append(queries, q)
		}
	}
	return queries
}
