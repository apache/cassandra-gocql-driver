// +build all integration

package gocql

// This file groups integration tests where Cassandra has to be set up with some special integration variables
import (
	"context"
	"reflect"
	"testing"
	"time"
)

// TestAuthentication verifies that gocql will work with a host configured to only accept authenticated connections
func TestAuthentication(t *testing.T) {

	if *flagProto < 2 {
		t.Skip("Authentication is not supported with protocol < 2")
	}

	if !*flagRunAuthTest {
		t.Skip("Authentication is not configured in the target cluster")
	}

	cluster := createCluster()

	cluster.Authenticator = PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	session, err := cluster.CreateSession()

	if err != nil {
		t.Fatalf("Authentication error: %s", err)
	}

	session.Close()
}

func TestGetHosts(t *testing.T) {
	clusterHosts := getClusterHosts()
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)

	hosts, partitioner, err := session.hostSource.GetHosts()

	assertTrue(t, "err == nil", err == nil)
	assertEqual(t, "len(hosts)", len(clusterHosts), len(hosts))
	assertTrue(t, "len(partitioner) != 0", len(partitioner) != 0)
}

//TestRingDiscovery makes sure that you can autodiscover other cluster members when you seed a cluster config with just one node
func TestRingDiscovery(t *testing.T) {
	clusterHosts := getClusterHosts()
	cluster := createCluster()
	cluster.Hosts = clusterHosts[:1]

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if *clusterSize > 1 {
		// wait for autodiscovery to update the pool with the list of known hosts
		time.Sleep(*flagAutoWait)
	}

	session.pool.mu.RLock()
	defer session.pool.mu.RUnlock()
	size := len(session.pool.hostConnPools)

	if *clusterSize != size {
		for p, pool := range session.pool.hostConnPools {
			t.Logf("p=%q host=%v ips=%s", p, pool.host, pool.host.ConnectAddress().String())

		}
		t.Errorf("Expected a cluster size of %d, but actual size was %d", *clusterSize, size)
	}
}

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
	session := createSession(t)
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

func TestCustomPayloadValues(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testCustomPayloadValues (id int, value int, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	values := []map[string][]byte{
		{"a": []byte{10, 20}, "b": []byte{20, 30}},
		nil,
		{"a": []byte{10, 20}, "b": nil},
	}

	for _, customPayload := range values {
		query := session.Query("SELECT id FROM testCustomPayloadValues where id = ?", 42).Consistency(One).CustomPayload(customPayload)
		iter := query.Iter()
		rCustomPayload := iter.GetCustomPayload()
		if !reflect.DeepEqual(customPayload, rCustomPayload) {
			t.Fatal("The received custom payload should match the sent")
		}
	}
}

func TestSessionAwaitSchemaAgreement(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
		t.Fatalf("expected session.AwaitSchemaAgreement to not return an error but got '%v'", err)
	}
}

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

// Integration test of querying and composition the keyspace metadata
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

func TestKeyspaceMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.test_metadata (first_id int, second_id int, third_id int, PRIMARY KEY (first_id, second_id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}
	createAggregate(t, session)
	createViews(t, session)
	createMaterializedViews(t, session)

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
}
