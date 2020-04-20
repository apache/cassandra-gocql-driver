// +build all cassandra
// +build !scylla

package gocql

import (
	"context"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestAggregateMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	createAggregate(t, session)

	aggregates, err := getAggregatesMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query aggregate metadata with err: %v", err)
	}
	if aggregates == nil {
		t.Fatal("failed to query aggregate metadata, nil returned")
	}
	if len(aggregates) != 1 {
		t.Fatal("expected only a single aggregate")
	}
	aggregate := aggregates[0]

	expectedAggregrate := AggregateMetadata{
		Keyspace:      "gocql_test",
		Name:          "average",
		ArgumentTypes: []TypeInfo{NativeType{typ: TypeInt}},
		InitCond:      "(0, 0)",
		ReturnType:    NativeType{typ: TypeDouble},
		StateType: TupleTypeInfo{
			NativeType: NativeType{typ: TypeTuple},

			Elems: []TypeInfo{
				NativeType{typ: TypeInt},
				NativeType{typ: TypeBigInt},
			},
		},
		stateFunc: "avgstate",
		finalFunc: "avgfinal",
	}

	// In this case cassandra is returning a blob
	if flagCassVersion.Before(3, 0, 0) {
		expectedAggregrate.InitCond = string([]byte{0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0})
	}

	if !reflect.DeepEqual(aggregate, expectedAggregrate) {
		t.Fatalf("aggregate is %+v, but expected %+v", aggregate, expectedAggregrate)
	}
}

func TestDiscoverViaProxy(t *testing.T) {
	// This (complicated) test tests that when the driver is given an initial host
	// that is infact a proxy it discovers the rest of the ring behind the proxy
	// and does not store the proxies address as a host in its connection pool.
	// See https://github.com/gocql/gocql/issues/481
	clusterHosts := getClusterHosts()
	proxy, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("unable to create proxy listener: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu         sync.Mutex
		proxyConns []net.Conn
		closed     bool
	)

	go func() {
		cassandraAddr := JoinHostPort(clusterHosts[0], 9042)

		cassandra := func() (net.Conn, error) {
			return net.Dial("tcp", cassandraAddr)
		}

		proxyFn := func(errs chan error, from, to net.Conn) {
			_, err := io.Copy(to, from)
			if err != nil {
				errs <- err
			}
		}

		// handle dials cassandra and then proxies requests and reponsess. It waits
		// for both the read and write side of the TCP connection to close before
		// returning.
		handle := func(conn net.Conn) error {
			cass, err := cassandra()
			if err != nil {
				return err
			}
			defer cass.Close()

			errs := make(chan error, 2)
			go proxyFn(errs, conn, cass)
			go proxyFn(errs, cass, conn)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-errs:
				return err
			}
		}

		for {
			// proxy just accepts connections and then proxies them to cassandra,
			// it runs until it is closed.
			conn, err := proxy.Accept()
			if err != nil {
				mu.Lock()
				if !closed {
					t.Error(err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			proxyConns = append(proxyConns, conn)
			mu.Unlock()

			go func(conn net.Conn) {
				defer conn.Close()

				if err := handle(conn); err != nil {
					mu.Lock()
					if !closed {
						t.Error(err)
					}
					mu.Unlock()
				}
			}(conn)
		}
	}()

	proxyAddr := proxy.Addr().String()

	cluster := createCluster()
	cluster.NumConns = 1
	// initial host is the proxy address
	cluster.Hosts = []string{proxyAddr}

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	// we shouldnt need this but to be safe
	time.Sleep(1 * time.Second)

	session.pool.mu.RLock()
	for _, host := range clusterHosts {
		if _, ok := session.pool.hostConnPools[host]; !ok {
			t.Errorf("missing host in pool after discovery: %q", host)
		}
	}
	session.pool.mu.RUnlock()

	mu.Lock()
	closed = true
	if err := proxy.Close(); err != nil {
		t.Log(err)
	}

	for _, conn := range proxyConns {
		if err := conn.Close(); err != nil {
			t.Log(err)
		}
	}
	mu.Unlock()
}

func TestFunctionMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	createFunctions(t, session)

	functions, err := getFunctionsMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query function metadata with err: %v", err)
	}
	if functions == nil {
		t.Fatal("failed to query function metadata, nil returned")
	}
	if len(functions) != 2 {
		t.Fatal("expected two functions")
	}
	avgState := functions[1]
	avgFinal := functions[0]

	avgStateBody := "if (val !=null) {state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue());}return state;"
	expectedAvgState := FunctionMetadata{
		Keyspace: "gocql_test",
		Name:     "avgstate",
		ArgumentTypes: []TypeInfo{
			TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},

				Elems: []TypeInfo{
					NativeType{typ: TypeInt},
					NativeType{typ: TypeBigInt},
				},
			},
			NativeType{typ: TypeInt},
		},
		ArgumentNames: []string{"state", "val"},
		ReturnType: TupleTypeInfo{
			NativeType: NativeType{typ: TypeTuple},

			Elems: []TypeInfo{
				NativeType{typ: TypeInt},
				NativeType{typ: TypeBigInt},
			},
		},
		CalledOnNullInput: true,
		Language:          "java",
		Body:              avgStateBody,
	}
	if !reflect.DeepEqual(avgState, expectedAvgState) {
		t.Fatalf("function is %+v, but expected %+v", avgState, expectedAvgState)
	}

	finalStateBody := "double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);"
	expectedAvgFinal := FunctionMetadata{
		Keyspace: "gocql_test",
		Name:     "avgfinal",
		ArgumentTypes: []TypeInfo{
			TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},

				Elems: []TypeInfo{
					NativeType{typ: TypeInt},
					NativeType{typ: TypeBigInt},
				},
			},
		},
		ArgumentNames:     []string{"state"},
		ReturnType:        NativeType{typ: TypeDouble},
		CalledOnNullInput: true,
		Language:          "java",
		Body:              finalStateBody,
	}
	if !reflect.DeepEqual(avgFinal, expectedAvgFinal) {
		t.Fatalf("function is %+v, but expected %+v", avgFinal, expectedAvgFinal)
	}
}

// Integration test of querying and composition the keyspace metadata
func TestKeyspaceMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.test_metadata (first_id int, second_id int, third_id int, PRIMARY KEY (first_id, second_id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	createAggregate(t, session)
	createViews(t, session)

	if err := session.Query("CREATE INDEX index_metadata ON test_metadata ( third_id )").Exec(); err != nil {
		t.Fatalf("failed to create index with err: %v", err)
	}

	keyspaceMetadata, err := session.KeyspaceMetadata("gocql_test")
	if err != nil {
		t.Fatalf("failed to query keyspace metadata with err: %v", err)
	}
	if keyspaceMetadata == nil {
		t.Fatal("expected the keyspace metadata to not be nil, but it was nil")
	}
	if keyspaceMetadata.Name != session.cfg.Keyspace {
		t.Fatalf("Expected the keyspace name to be %s but was %s", session.cfg.Keyspace, keyspaceMetadata.Name)
	}
	if len(keyspaceMetadata.Tables) == 0 {
		t.Errorf("Expected tables but there were none")
	}

	tableMetadata, found := keyspaceMetadata.Tables["test_metadata"]
	if !found {
		t.Fatalf("failed to find the test_metadata table metadata")
	}

	if len(tableMetadata.PartitionKey) != 1 {
		t.Errorf("expected partition key length of 1, but was %d", len(tableMetadata.PartitionKey))
	}
	for i, column := range tableMetadata.PartitionKey {
		if column == nil {
			t.Errorf("partition key column metadata at index %d was nil", i)
		}
	}
	if tableMetadata.PartitionKey[0].Name != "first_id" {
		t.Errorf("Expected the first partition key column to be 'first_id' but was '%s'", tableMetadata.PartitionKey[0].Name)
	}
	if len(tableMetadata.ClusteringColumns) != 1 {
		t.Fatalf("expected clustering columns length of 1, but was %d", len(tableMetadata.ClusteringColumns))
	}
	for i, column := range tableMetadata.ClusteringColumns {
		if column == nil {
			t.Fatalf("clustering column metadata at index %d was nil", i)
		}
	}
	if tableMetadata.ClusteringColumns[0].Name != "second_id" {
		t.Errorf("Expected the first clustering column to be 'second_id' but was '%s'", tableMetadata.ClusteringColumns[0].Name)
	}
	thirdColumn, found := tableMetadata.Columns["third_id"]
	if !found {
		t.Fatalf("Expected a column definition for 'third_id'")
	}
	if !session.useSystemSchema && thirdColumn.Index.Name != "index_metadata" {
		// TODO(zariel): scan index info from system_schema
		t.Errorf("Expected column index named 'index_metadata' but was '%s'", thirdColumn.Index.Name)
	}

	aggregate, found := keyspaceMetadata.Aggregates["average"]
	if !found {
		t.Fatal("failed to find the aggreate in metadata")
	}
	if aggregate.FinalFunc.Name != "avgfinal" {
		t.Fatalf("expected final function %s, but got %s", "avgFinal", aggregate.FinalFunc.Name)
	}
	if aggregate.StateFunc.Name != "avgstate" {
		t.Fatalf("expected state function %s, but got %s", "avgstate", aggregate.StateFunc.Name)
	}

	_, found = keyspaceMetadata.Views["basicview"]
	if !found {
		t.Fatal("failed to find the view in metadata")
	}
}

func TestLexicalUUIDType(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.test_lexical_uuid (
			key     varchar,
			column1 'org.apache.cassandra.db.marshal.LexicalUUIDType',
			value   int,
			PRIMARY KEY (key, column1)
		)`); err != nil {
		t.Fatal("create:", err)
	}

	key := TimeUUID().String()
	column1 := TimeUUID()

	err := session.Query("INSERT INTO test_lexical_uuid(key, column1, value) VALUES(?, ?, ?)", key, column1, 55).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var gotUUID UUID
	if err := session.Query("SELECT column1 from test_lexical_uuid where key = ? AND column1 = ?", key, column1).Scan(&gotUUID); err != nil {
		t.Fatal(err)
	}

	if gotUUID != column1 {
		t.Errorf("got %s, expected %s", gotUUID, column1)
	}
}
