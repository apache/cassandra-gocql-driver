// +build all cassandra
// +build !scylla

package gocql

import (
	"context"
	"io"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

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

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES exist.
func TestGetKeyspaceMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	keyspaceMetadata, err := getKeyspaceMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query the keyspace metadata with err: %v", err)
	}
	if keyspaceMetadata == nil {
		t.Fatal("failed to query the keyspace metadata, nil returned")
	}
	if keyspaceMetadata.Name != "gocql_test" {
		t.Errorf("Expected keyspace name to be 'gocql' but was '%s'", keyspaceMetadata.Name)
	}
	if keyspaceMetadata.StrategyClass != "org.apache.cassandra.locator.SimpleStrategy" {
		t.Errorf("Expected replication strategy class to be 'org.apache.cassandra.locator.SimpleStrategy' but was '%s'", keyspaceMetadata.StrategyClass)
	}
	if keyspaceMetadata.StrategyOptions == nil {
		t.Error("Expected replication strategy options map but was nil")
	}
	rfStr, ok := keyspaceMetadata.StrategyOptions["replication_factor"]
	if !ok {
		t.Fatalf("Expected strategy option 'replication_factor' but was not found in %v", keyspaceMetadata.StrategyOptions)
	}
	rfInt, err := strconv.Atoi(rfStr.(string))
	if err != nil {
		t.Fatalf("Error converting string to int with err: %v", err)
	}
	if rfInt != *flagRF {
		t.Errorf("Expected replication factor to be %d but was %d", *flagRF, rfInt)
	}
}

// Integration test of just querying for data from the system.schema_keyspace table where the keyspace DOES NOT exist.
func TestGetKeyspaceMetadataFails(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	_, err := getKeyspaceMetadata(session, "gocql_keyspace_does_not_exist")

	if err != ErrKeyspaceDoesNotExist || err == nil {
		t.Fatalf("Expected error of type ErrKeySpaceDoesNotExist. Instead, error was %v", err)
	}
}

// Integration test of just querying for data from the system.schema_columnfamilies table
func TestGetTableMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.test_table_metadata (first_id int, second_id int, third_id int, PRIMARY KEY (first_id, second_id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	tables, err := getTableMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query the table metadata with err: %v", err)
	}
	if tables == nil {
		t.Fatal("failed to query the table metadata, nil returned")
	}

	var testTable *TableMetadata

	// verify all tables have minimum expected data
	for i := range tables {
		table := &tables[i]

		if table.Name == "" {
			t.Errorf("Expected table name to be set, but it was empty: index=%d metadata=%+v", i, table)
		}
		if table.Keyspace != "gocql_test" {
			t.Errorf("Expected keyspace for '%s' table metadata to be 'gocql_test' but was '%s'", table.Name, table.Keyspace)
		}
		if session.cfg.ProtoVersion < 4 {
			// TODO(zariel): there has to be a better way to detect what metadata version
			// we are in, and a better way to structure the code so that it is abstracted away
			// from us here
			if table.KeyValidator == "" {
				t.Errorf("Expected key validator to be set for table %s", table.Name)
			}
			if table.Comparator == "" {
				t.Errorf("Expected comparator to be set for table %s", table.Name)
			}
			if table.DefaultValidator == "" {
				t.Errorf("Expected default validator to be set for table %s", table.Name)
			}
		}

		// these fields are not set until the metadata is compiled
		if table.PartitionKey != nil {
			t.Errorf("Did not expect partition key for table %s", table.Name)
		}
		if table.ClusteringColumns != nil {
			t.Errorf("Did not expect clustering columns for table %s", table.Name)
		}
		if table.Columns != nil {
			t.Errorf("Did not expect columns for table %s", table.Name)
		}

		// for the next part of the test after this loop, find the metadata for the test table
		if table.Name == "test_table_metadata" {
			testTable = table
		}
	}

	// verify actual values on the test tables
	if testTable == nil {
		t.Fatal("Expected table metadata for name 'test_table_metadata'")
	}
	if session.cfg.ProtoVersion == protoVersion1 {
		if testTable.KeyValidator != "org.apache.cassandra.db.marshal.Int32Type" {
			t.Errorf("Expected test_table_metadata key validator to be 'org.apache.cassandra.db.marshal.Int32Type' but was '%s'", testTable.KeyValidator)
		}
		if testTable.Comparator != "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)" {
			t.Errorf("Expected test_table_metadata key validator to be 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)' but was '%s'", testTable.Comparator)
		}
		if testTable.DefaultValidator != "org.apache.cassandra.db.marshal.BytesType" {
			t.Errorf("Expected test_table_metadata key validator to be 'org.apache.cassandra.db.marshal.BytesType' but was '%s'", testTable.DefaultValidator)
		}
		expectedKeyAliases := []string{"first_id"}
		if !reflect.DeepEqual(testTable.KeyAliases, expectedKeyAliases) {
			t.Errorf("Expected key aliases %v but was %v", expectedKeyAliases, testTable.KeyAliases)
		}
		expectedColumnAliases := []string{"second_id"}
		if !reflect.DeepEqual(testTable.ColumnAliases, expectedColumnAliases) {
			t.Errorf("Expected key aliases %v but was %v", expectedColumnAliases, testTable.ColumnAliases)
		}
	}
	if testTable.ValueAlias != "" {
		t.Errorf("Expected value alias '' but was '%s'", testTable.ValueAlias)
	}
}

// Integration test of just querying for data from the system.schema_columns table
func TestGetColumnMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.test_column_metadata (first_id int, second_id int, third_id int, PRIMARY KEY (first_id, second_id))"); err != nil {
		t.Fatalf("failed to create table with error '%v'", err)
	}

	if err := session.Query("CREATE INDEX index_column_metadata ON test_column_metadata ( third_id )").Exec(); err != nil {
		t.Fatalf("failed to create index with err: %v", err)
	}

	columns, err := getColumnMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query column metadata with err: %v", err)
	}
	if columns == nil {
		t.Fatal("failed to query column metadata, nil returned")
	}

	testColumns := map[string]*ColumnMetadata{}

	// verify actual values on the test columns
	for i := range columns {
		column := &columns[i]

		if column.Name == "" {
			t.Errorf("Expected column name to be set, but it was empty: index=%d metadata=%+v", i, column)
		}
		if column.Table == "" {
			t.Errorf("Expected column %s table name to be set, but it was empty", column.Name)
		}
		if column.Keyspace != "gocql_test" {
			t.Errorf("Expected column %s keyspace name to be 'gocql_test', but it was '%s'", column.Name, column.Keyspace)
		}
		if column.Kind == ColumnUnkownKind {
			t.Errorf("Expected column %s kind to be set, but it was empty", column.Name)
		}
		if session.cfg.ProtoVersion == 1 && column.Kind != ColumnRegular {
			t.Errorf("Expected column %s kind to be set to 'regular' for proto V1 but it was '%s'", column.Name, column.Kind)
		}
		if column.Validator == "" {
			t.Errorf("Expected column %s validator to be set, but it was empty", column.Name)
		}

		// find the test table columns for the next step after this loop
		if column.Table == "test_column_metadata" {
			testColumns[column.Name] = column
		}
	}

	if session.cfg.ProtoVersion == 1 {
		// V1 proto only returns "regular columns"
		if len(testColumns) != 1 {
			t.Errorf("Expected 1 test columns but there were %d", len(testColumns))
		}
		thirdID, found := testColumns["third_id"]
		if !found {
			t.Fatalf("Expected to find column 'third_id' metadata but there was only %v", testColumns)
		}

		if thirdID.Kind != ColumnRegular {
			t.Errorf("Expected %s column kind to be '%s' but it was '%s'", thirdID.Name, ColumnRegular, thirdID.Kind)
		}

		if thirdID.Index.Name != "index_column_metadata" {
			t.Errorf("Expected %s column index name to be 'index_column_metadata' but it was '%s'", thirdID.Name, thirdID.Index.Name)
		}
	} else {
		if len(testColumns) != 3 {
			t.Errorf("Expected 3 test columns but there were %d", len(testColumns))
		}
		firstID, found := testColumns["first_id"]
		if !found {
			t.Fatalf("Expected to find column 'first_id' metadata but there was only %v", testColumns)
		}
		secondID, found := testColumns["second_id"]
		if !found {
			t.Fatalf("Expected to find column 'second_id' metadata but there was only %v", testColumns)
		}
		thirdID, found := testColumns["third_id"]
		if !found {
			t.Fatalf("Expected to find column 'third_id' metadata but there was only %v", testColumns)
		}

		if firstID.Kind != ColumnPartitionKey {
			t.Errorf("Expected %s column kind to be '%s' but it was '%s'", firstID.Name, ColumnPartitionKey, firstID.Kind)
		}
		if secondID.Kind != ColumnClusteringKey {
			t.Errorf("Expected %s column kind to be '%s' but it was '%s'", secondID.Name, ColumnClusteringKey, secondID.Kind)
		}
		if thirdID.Kind != ColumnRegular {
			t.Errorf("Expected %s column kind to be '%s' but it was '%s'", thirdID.Name, ColumnRegular, thirdID.Kind)
		}

		if !session.useSystemSchema && thirdID.Index.Name != "index_column_metadata" {
			// TODO(zariel): update metadata to scan index from system_schema
			t.Errorf("Expected %s column index name to be 'index_column_metadata' but it was '%s'", thirdID.Name, thirdID.Index.Name)
		}
	}
}

func TestViewMetadata(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	createViews(t, session)

	views, err := getViewsMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query view metadata with err: %v", err)
	}
	if views == nil {
		t.Fatal("failed to query view metadata, nil returned")
	}

	if len(views) != 1 {
		t.Fatal("expected one view")
	}

	textType := TypeText
	if flagCassVersion.Before(3, 0, 0) {
		textType = TypeVarchar
	}

	expectedView := ViewMetadata{
		Keyspace:   "gocql_test",
		Name:       "basicview",
		FieldNames: []string{"birthday", "nationality", "weight", "height"},
		FieldTypes: []TypeInfo{
			NativeType{typ: TypeTimestamp},
			NativeType{typ: textType},
			NativeType{typ: textType},
			NativeType{typ: textType},
		},
	}

	if !reflect.DeepEqual(views[0], expectedView) {
		t.Fatalf("view is %+v, but expected %+v", views[0], expectedView)
	}
}

func TestMaterializedViewMetadata(t *testing.T) {
	if flagCassVersion.Before(3, 0, 0) {
		return
	}
	session := createSession(t)
	defer session.Close()
	createMaterializedViews(t, session)

	materializedViews, err := getMaterializedViewsMetadata(session, "gocql_test")
	if err != nil {
		t.Fatalf("failed to query view metadata with err: %v", err)
	}
	if materializedViews == nil {
		t.Fatal("failed to query view metadata, nil returned")
	}
	if len(materializedViews) != 2 {
		t.Fatal("expected two views")
	}
	expectedView1 := MaterializedViewMetadata{
		Keyspace:                "gocql_test",
		Name:                    "view_view",
		baseTableName:           "view_table",
		BloomFilterFpChance:     0.01,
		Caching:                 map[string]string{"keys": "ALL", "rows_per_partition": "NONE"},
		Comment:                 "",
		Compaction:              map[string]string{"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold": "32", "min_threshold": "4"},
		Compression:             map[string]string{"chunk_length_in_kb": "64", "class": "org.apache.cassandra.io.compress.LZ4Compressor"},
		CrcCheckChance:          1,
		DcLocalReadRepairChance: 0.1,
		DefaultTimeToLive:       0,
		Extensions:              map[string]string{},
		GcGraceSeconds:          864000,
		IncludeAllColumns:       false, MaxIndexInterval: 2048, MemtableFlushPeriodInMs: 0, MinIndexInterval: 128, ReadRepairChance: 0,
		SpeculativeRetry: "99PERCENTILE",
	}
	expectedView2 := MaterializedViewMetadata{
		Keyspace:                "gocql_test",
		Name:                    "view_view2",
		baseTableName:           "view_table2",
		BloomFilterFpChance:     0.01,
		Caching:                 map[string]string{"keys": "ALL", "rows_per_partition": "NONE"},
		Comment:                 "",
		Compaction:              map[string]string{"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold": "32", "min_threshold": "4"},
		Compression:             map[string]string{"chunk_length_in_kb": "64", "class": "org.apache.cassandra.io.compress.LZ4Compressor"},
		CrcCheckChance:          1,
		DcLocalReadRepairChance: 0.1,
		DefaultTimeToLive:       0,
		Extensions:              map[string]string{},
		GcGraceSeconds:          864000,
		IncludeAllColumns:       false, MaxIndexInterval: 2048, MemtableFlushPeriodInMs: 0, MinIndexInterval: 128, ReadRepairChance: 0,
		SpeculativeRetry: "99PERCENTILE",
	}

	expectedView1.BaseTableId = materializedViews[0].BaseTableId
	expectedView1.Id = materializedViews[0].Id
	if !reflect.DeepEqual(materializedViews[0], expectedView1) {
		t.Fatalf("materialized view is %+v, but expected %+v", materializedViews[0], expectedView1)
	}
	expectedView2.BaseTableId = materializedViews[1].BaseTableId
	expectedView2.Id = materializedViews[1].Id
	if !reflect.DeepEqual(materializedViews[1], expectedView2) {
		t.Fatalf("materialized view is %+v, but expected %+v", materializedViews[1], expectedView2)
	}
}

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
	if len(aggregates) != 2 {
		t.Fatal("expected two aggregates")
	}

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

	if !reflect.DeepEqual(aggregates[0], expectedAggregrate) {
		t.Fatalf("aggregate 'average' is %+v, but expected %+v", aggregates[0], expectedAggregrate)
	}
	expectedAggregrate.Name = "average2"
	if !reflect.DeepEqual(aggregates[1], expectedAggregrate) {
		t.Fatalf("aggregate 'average2' is %+v, but expected %+v", aggregates[1], expectedAggregrate)
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

// Integration test of querying and composition the keyspace metadata
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
		t.Fatal("failed to find the aggregate 'average' in metadata")
	}
	if aggregate.FinalFunc.Name != "avgfinal" {
		t.Fatalf("expected final function %s, but got %s", "avgFinal", aggregate.FinalFunc.Name)
	}
	if aggregate.StateFunc.Name != "avgstate" {
		t.Fatalf("expected state function %s, but got %s", "avgstate", aggregate.StateFunc.Name)
	}
	aggregate, found = keyspaceMetadata.Aggregates["average2"]
	if !found {
		t.Fatal("failed to find the aggregate 'average2' in metadata")
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
	_, found = keyspaceMetadata.UserTypes["basicview"]
	if !found {
		t.Fatal("failed to find the types in metadata")
	}
	textType := TypeText
	if flagCassVersion.Before(3, 0, 0) {
		textType = TypeVarchar
	}
	expectedType := UserTypeMetadata{
		Keyspace:   "gocql_test",
		Name:       "basicview",
		FieldNames: []string{"birthday", "nationality", "weight", "height"},
		FieldTypes: []TypeInfo{
			NativeType{typ: TypeTimestamp},
			NativeType{typ: textType},
			NativeType{typ: textType},
			NativeType{typ: textType},
		},
	}
	if !reflect.DeepEqual(*keyspaceMetadata.UserTypes["basicview"], expectedType) {
		t.Fatalf("type is %+v, but expected %+v", keyspaceMetadata.UserTypes["basicview"], expectedType)
	}
	if flagCassVersion.Major >= 3 {
		materializedView, found := keyspaceMetadata.MaterializedViews["view_view"]
		if !found {
			t.Fatal("failed to find materialized view view_view in metadata")
		}
		if materializedView.BaseTable.Name != "view_table" {
			t.Fatalf("expected name: %s, materialized view base table name: %s", "view_table", materializedView.BaseTable.Name)
		}
		materializedView, found = keyspaceMetadata.MaterializedViews["view_view2"]
		if !found {
			t.Fatal("failed to find materialized view view_view2 in metadata")
		}
		if materializedView.BaseTable.Name != "view_table2" {
			t.Fatalf("expected name: %s, materialized view base table name: %s", "view_table2", materializedView.BaseTable.Name)
		}
	}
}
