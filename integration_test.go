//go:build all || integration
// +build all integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

// This file groups integration tests where Cassandra has to be set up with some special integration variables
import (
	"context"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	inf "gopkg.in/inf.v0"
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

// TestRingDiscovery makes sure that you can autodiscover other cluster members
// when you seed a cluster config with just one node
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

// TestHostFilterDiscovery ensures that host filtering works even when we discover hosts
func TestHostFilterDiscovery(t *testing.T) {
	clusterHosts := getClusterHosts()
	if len(clusterHosts) < 2 {
		t.Skip("skipping because we don't have 2 or more hosts")
	}
	cluster := createCluster()
	rr := RoundRobinHostPolicy().(*roundRobinHostPolicy)
	cluster.PoolConfig.HostSelectionPolicy = rr
	// we'll filter out the second host
	filtered := clusterHosts[1]
	cluster.Hosts = clusterHosts[:1]
	cluster.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
		if host.ConnectAddress().String() == filtered {
			return false
		}
		return true
	})
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	assertEqual(t, "len(clusterHosts)-1 != len(rr.hosts.get())", len(clusterHosts)-1, len(rr.hosts.get()))
}

// TestHostFilterInitial ensures that host filtering works for the initial
// connection including the control connection
func TestHostFilterInitial(t *testing.T) {
	clusterHosts := getClusterHosts()
	if len(clusterHosts) < 2 {
		t.Skip("skipping because we don't have 2 or more hosts")
	}
	cluster := createCluster()
	rr := RoundRobinHostPolicy().(*roundRobinHostPolicy)
	cluster.PoolConfig.HostSelectionPolicy = rr
	// we'll filter out the second host
	filtered := clusterHosts[1]
	cluster.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
		if host.ConnectAddress().String() == filtered {
			return false
		}
		return true
	})
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	assertEqual(t, "len(clusterHosts)-1 != len(rr.hosts.get())", len(clusterHosts)-1, len(rr.hosts.get()))
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
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)
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
		t.Fatalf("The received custom payload %#v should match the sent %#v", rCustomPayload, customPayload)
	}
	iter.Close()

	// Insert query
	query = session.Query("INSERT INTO testCustomPayloadMessages(id,value) VALUES(1, 1)").Consistency(One).CustomPayload(customPayload)
	iter = query.Iter()
	rCustomPayload = iter.GetCustomPayload()
	if !reflect.DeepEqual(customPayload, rCustomPayload) {
		t.Fatalf("The received custom payload %#v should match the sent %#v", rCustomPayload, customPayload)
	}
	iter.Close()

	// Batch Message
	b := session.Batch(LoggedBatch)
	b.CustomPayload = customPayload
	b.Query("INSERT INTO testCustomPayloadMessages(id,value) VALUES(1, 1)")
	if err := b.Exec(); err != nil {
		t.Fatalf("query failed. %v", err)
	}
}

func TestCustomPayloadValues(t *testing.T) {
	cluster := createCluster()
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.testCustomPayloadValues (id int, value int, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}

	values := []map[string][]byte{map[string][]byte{"a": []byte{10, 20}, "b": []byte{20, 30}}, nil, map[string][]byte{"a": []byte{10, 20}, "b": nil}}

	for _, customPayload := range values {
		query := session.Query("SELECT id FROM testCustomPayloadValues where id = ?", 42).Consistency(One).CustomPayload(customPayload)
		iter := query.Iter()
		rCustomPayload := iter.GetCustomPayload()
		if !reflect.DeepEqual(customPayload, rCustomPayload) {
			t.Fatalf("The received custom payload %#v should match the sent %#v", rCustomPayload, customPayload)
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

// SliceMapTypesTestCase defines a test case for validating SliceMap/MapScan behavior
type SliceMapTypesTestCase struct {
	CQLType           string
	CQLValue          string      // Non-NULL value to insert
	ExpectedValue     interface{} // Expected value for non-NULL case
	ExpectedNullValue interface{} // Expected value for NULL
}

// compareCollectionValues compares collection values (lists, sets, maps) with special handling
func compareCollectionValues(t *testing.T, cqlType string, expected, actual interface{}) bool {
	switch {
	case strings.HasPrefix(cqlType, "set<"):
		// Sets are returned as slices, but order is not guaranteed
		expectedSlice := reflect.ValueOf(expected)
		actualSlice := reflect.ValueOf(actual)
		if expectedSlice.Kind() != reflect.Slice || actualSlice.Kind() != reflect.Slice {
			return false
		}
		if expectedSlice.Len() != actualSlice.Len() {
			return false
		}

		// Convert to maps for unordered comparison
		expectedSet := make(map[interface{}]bool)
		for i := 0; i < expectedSlice.Len(); i++ {
			expectedSet[expectedSlice.Index(i).Interface()] = true
		}

		actualSet := make(map[interface{}]bool)
		for i := 0; i < actualSlice.Len(); i++ {
			actualSet[actualSlice.Index(i).Interface()] = true
		}

		return reflect.DeepEqual(expectedSet, actualSet)

	default:
		// For lists, maps, and other collections, reflect.DeepEqual works fine
		return reflect.DeepEqual(expected, actual)
	}
}

// compareValues compares expected and actual values with type-specific logic
func compareValues(t *testing.T, cqlType string, expected, actual interface{}) bool {
	switch cqlType {
	case "varint":
		// big.Int needs Cmp() for proper comparison, but handle nil pointers safely
		if expectedBig, ok := expected.(*big.Int); ok {
			if actualBig, ok := actual.(*big.Int); ok {
				// Handle nil cases
				if expectedBig == nil && actualBig == nil {
					return true
				}
				if expectedBig == nil || actualBig == nil {
					return false
				}
				return expectedBig.Cmp(actualBig) == 0
			}
		}
		return reflect.DeepEqual(expected, actual)

	case "decimal":
		// inf.Dec needs Cmp() for proper comparison, but handle nil pointers safely
		if expectedDec, ok := expected.(*inf.Dec); ok {
			if actualDec, ok := actual.(*inf.Dec); ok {
				// Handle nil cases
				if expectedDec == nil && actualDec == nil {
					return true
				}
				if expectedDec == nil || actualDec == nil {
					return false
				}
				return expectedDec.Cmp(actualDec) == 0
			}
		}
		return reflect.DeepEqual(expected, actual)

	default:
		// reflect.DeepEqual handles nil vs empty slice/map distinction correctly for all types
		// including inet (net.IP), blob ([]byte), collections ([]T, map[K]V), etc.
		// This is critical for catching zero value behavior changes in the driver
		return reflect.DeepEqual(expected, actual)
	}
}

// TestSliceMapMapScanTypes tests SliceMap and MapScan with various CQL types
func TestSliceMapMapScanTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Create test table
	tableCQL := `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_test (
			id int PRIMARY KEY,
			tinyint_col tinyint,
			smallint_col smallint,
			int_col int,
			bigint_col bigint,
			float_col float,
			double_col double,
			boolean_col boolean,
			text_col text,
			ascii_col ascii,
			varchar_col varchar,
			timestamp_col timestamp,
			uuid_col uuid,
			timeuuid_col timeuuid,
			inet_col inet,
			blob_col blob,
			varint_col varint,
			decimal_col decimal,
			date_col date,
			time_col time,
			duration_col duration
		)`

	if err := createTable(session, tableCQL); err != nil {
		t.Fatal("Failed to create test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_test").Exec(); err != nil {
		t.Fatal("Failed to truncate test table:", err)
	}

	testCases := []SliceMapTypesTestCase{
		{"tinyint", "42", int8(42), int8(0)},
		{"smallint", "1234", int16(1234), int16(0)},
		{"int", "123456", int(123456), int(0)},
		{"bigint", "1234567890", int64(1234567890), int64(0)},
		{"float", "3.14", float32(3.14), float32(0)},
		{"double", "2.718281828", float64(2.718281828), float64(0)},
		{"boolean", "true", true, false},
		{"text", "'hello world'", "hello world", ""},
		{"ascii", "'hello ascii'", "hello ascii", ""},
		{"varchar", "'hello varchar'", "hello varchar", ""},
		{"timestamp", "1388534400000", time.Unix(1388534400, 0).UTC(), time.Time{}},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000", mustParseUUID("550e8400-e29b-41d4-a716-446655440000"), UUID{}},
		{"timeuuid", "60d79c23-5793-11f0-8afe-bcfce78b517a", mustParseUUID("60d79c23-5793-11f0-8afe-bcfce78b517a"), UUID{}},
		{"inet", "'127.0.0.1'", net.ParseIP("127.0.0.1").To4(), net.IP(nil)},
		{"blob", "0x48656c6c6f", []byte("Hello"), []byte(nil)},
		{"varint", "123456789012345678901234567890", mustParseBigInt("123456789012345678901234567890"), (*big.Int)(nil)},
		{"decimal", "123.45", mustParseDecimal("123.45"), (*inf.Dec)(nil)},
		{"date", "'2015-05-03'", time.Date(2015, 5, 3, 0, 0, 0, 0, time.UTC), time.Time{}},
		{"time", "'13:30:54.234'", 13*time.Hour + 30*time.Minute + 54*time.Second + 234*time.Millisecond, time.Duration(0)},
		{"duration", "1y2mo3d4h5m6s789ms", mustCreateDuration(14, 3, 4*time.Hour+5*time.Minute+6*time.Second+789*time.Millisecond), Duration{}},
	}

	for i, tc := range testCases {
		t.Run(tc.CQLType, func(t *testing.T) {
			testSliceMapMapScanSimple(t, session, tc, i)
		})
	}
}

// Simplified test function that tests both SliceMap and MapScan with both NULL and non-NULL values
func testSliceMapMapScanSimple(t *testing.T, session *Session, tc SliceMapTypesTestCase, id int) {
	colName := tc.CQLType + "_col"

	// Test non-NULL value
	t.Run("NonNull", func(t *testing.T) {
		// Insert non-NULL value
		insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_test (id, %s) VALUES (?, %s)", colName, tc.CQLValue)
		if err := session.Query(insertQuery, id*2).Exec(); err != nil {
			t.Fatalf("Failed to insert non-NULL value: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				result := queryAndExtractValue(t, session, colName, id*2, method)
				validateResult(t, tc.CQLType, tc.ExpectedValue, result, method, "non-NULL")
			})
		}
	})

	// Test NULL value
	t.Run("Null", func(t *testing.T) {
		// Insert NULL value
		insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_test (id, %s) VALUES (?, NULL)", colName)
		if err := session.Query(insertQuery, id*2+1).Exec(); err != nil {
			t.Fatalf("Failed to insert NULL value: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				result := queryAndExtractValue(t, session, colName, id*2+1, method)
				validateResult(t, tc.CQLType, tc.ExpectedNullValue, result, method, "NULL")
			})
		}
	})
}

// Helper function to query and extract value using either SliceMap or MapScan
func queryAndExtractValue(t *testing.T, session *Session, colName string, id int, method string) interface{} {
	selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_test WHERE id = ?", colName)

	switch method {
	case "SliceMap":
		iter := session.Query(selectQuery, id).Iter()
		sliceResults, err := iter.SliceMap()
		iter.Close()
		if err != nil {
			t.Fatalf("SliceMap failed: %v", err)
		}
		if len(sliceResults) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(sliceResults))
		}
		return sliceResults[0][colName]

	case "MapScan":
		mapResult := make(map[string]interface{})
		if err := session.Query(selectQuery, id).MapScan(mapResult); err != nil {
			t.Fatalf("MapScan failed: %v", err)
		}
		return mapResult[colName]

	default:
		t.Fatalf("Unknown method: %s", method)
		return nil
	}
}

// Helper function to validate results
func validateResult(t *testing.T, cqlType string, expected, actual interface{}, method, valueType string) {
	// Check type
	if expected != nil && actual != nil {
		expectedType := reflect.TypeOf(expected)
		actualType := reflect.TypeOf(actual)
		if expectedType != actualType {
			t.Errorf("%s %s %s: expected type %v, got %v", method, valueType, cqlType, expectedType, actualType)
		}
	}

	// Check value
	if !compareValues(t, cqlType, expected, actual) {
		t.Errorf("%s %s %s: expected value %v (type %T), got %v (type %T)",
			method, valueType, cqlType, expected, expected, actual, actual)
	}
}

// Helper function to parse UUID (for test data)
func mustParseUUID(s string) UUID {
	u, err := ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return u
}

// Helper function to parse big.Int (for test data)
func mustParseBigInt(s string) *big.Int {
	i := new(big.Int)
	if _, ok := i.SetString(s, 10); !ok {
		panic("failed to parse big.Int: " + s)
	}
	return i
}

// Helper function to parse inf.Dec (for test data)
func mustParseDecimal(s string) *inf.Dec {
	dec := new(inf.Dec)
	if _, ok := dec.SetString(s); !ok {
		panic("failed to parse inf.Dec: " + s)
	}
	return dec
}

// Helper function to create Duration (for test data)
func mustCreateDuration(months int32, days int32, timeDuration time.Duration) Duration {
	return Duration{
		Months:      months,
		Days:        days,
		Nanoseconds: timeDuration.Nanoseconds(),
	}
}

// TestSliceMapMapScanCounterTypes tests counter types separately since they have special restrictions
// (counter columns can't be mixed with other column types in the same table)
func TestSliceMapMapScanCounterTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Create separate table for counter types
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_counter_test (
			id int PRIMARY KEY,
			counter_col counter
		)
	`); err != nil {
		t.Fatal("Failed to create counter test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_counter_test").Exec(); err != nil {
		t.Fatal("Failed to truncate counter test table:", err)
	}

	testID := 1
	expectedValue := int64(42)

	// Increment counter (can't INSERT into counter, must UPDATE)
	err := session.Query("UPDATE gocql_test.slicemap_counter_test SET counter_col = counter_col + 42 WHERE id = ?", testID).Exec()
	if err != nil {
		t.Fatalf("Failed to increment counter: %v", err)
	}

	// Test both SliceMap and MapScan
	for _, method := range []string{"SliceMap", "MapScan"} {
		t.Run(method, func(t *testing.T) {
			var result interface{}

			selectQuery := "SELECT counter_col FROM gocql_test.slicemap_counter_test WHERE id = ?"
			if method == "SliceMap" {
				iter := session.Query(selectQuery, testID).Iter()
				sliceResults, err := iter.SliceMap()
				iter.Close()
				if err != nil {
					t.Fatalf("SliceMap failed: %v", err)
				}
				if len(sliceResults) != 1 {
					t.Fatalf("Expected 1 result, got %d", len(sliceResults))
				}
				result = sliceResults[0]["counter_col"]
			} else {
				mapResult := make(map[string]interface{})
				if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
					t.Fatalf("MapScan failed: %v", err)
				}
				result = mapResult["counter_col"]
			}

			validateResult(t, "counter", expectedValue, result, method, "incremented")
		})
	}
}

// TestSliceMapMapScanTupleTypes tests tuple types separately since they have special handling
// (tuple elements get split into individual columns)
func TestSliceMapMapScanTupleTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Create test table with tuple column
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_tuple_test (
			id int PRIMARY KEY,
			tuple_col tuple<int, text>
		)
	`); err != nil {
		t.Fatal("Failed to create tuple test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_tuple_test").Exec(); err != nil {
		t.Fatal("Failed to truncate tuple test table:", err)
	}

	// Test non-NULL tuple
	t.Run("NonNull", func(t *testing.T) {
		testID := 1
		// Insert tuple value
		err := session.Query("INSERT INTO gocql_test.slicemap_tuple_test (id, tuple_col) VALUES (?, (42, 'hello'))", testID).Exec()
		if err != nil {
			t.Fatalf("Failed to insert tuple value: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				var result map[string]interface{}

				selectQuery := "SELECT tuple_col FROM gocql_test.slicemap_tuple_test WHERE id = ?"
				if method == "SliceMap" {
					iter := session.Query(selectQuery, testID).Iter()
					sliceResults, err := iter.SliceMap()
					iter.Close()
					if err != nil {
						t.Fatalf("SliceMap failed: %v", err)
					}
					if len(sliceResults) != 1 {
						t.Fatalf("Expected 1 result, got %d", len(sliceResults))
					}
					result = sliceResults[0]
				} else {
					result = make(map[string]interface{})
					if err := session.Query(selectQuery, testID).MapScan(result); err != nil {
						t.Fatalf("MapScan failed: %v", err)
					}
				}

				// Check tuple elements (tuples get split into individual columns)
				elem0Key := TupleColumnName("tuple_col", 0)
				elem1Key := TupleColumnName("tuple_col", 1)

				if result[elem0Key] != 42 {
					t.Errorf("%s tuple[0]: expected 42, got %v", method, result[elem0Key])
				}
				if result[elem1Key] != "hello" {
					t.Errorf("%s tuple[1]: expected 'hello', got %v", method, result[elem1Key])
				}
			})
		}
	})

	// Test NULL tuple
	t.Run("Null", func(t *testing.T) {
		testID := 2
		// Insert NULL tuple
		err := session.Query("INSERT INTO gocql_test.slicemap_tuple_test (id, tuple_col) VALUES (?, NULL)", testID).Exec()
		if err != nil {
			t.Fatalf("Failed to insert NULL tuple: %v", err)
		}

		// Test both SliceMap and MapScan
		for _, method := range []string{"SliceMap", "MapScan"} {
			t.Run(method, func(t *testing.T) {
				var result map[string]interface{}

				selectQuery := "SELECT tuple_col FROM gocql_test.slicemap_tuple_test WHERE id = ?"
				if method == "SliceMap" {
					iter := session.Query(selectQuery, testID).Iter()
					sliceResults, err := iter.SliceMap()
					iter.Close()
					if err != nil {
						t.Fatalf("SliceMap failed: %v", err)
					}
					if len(sliceResults) != 1 {
						t.Fatalf("Expected 1 result, got %d", len(sliceResults))
					}
					result = sliceResults[0]
				} else {
					result = make(map[string]interface{})
					if err := session.Query(selectQuery, testID).MapScan(result); err != nil {
						t.Fatalf("MapScan failed: %v", err)
					}
				}

				// Check tuple elements (NULL tuple gives zero values)
				elem0Key := TupleColumnName("tuple_col", 0)
				elem1Key := TupleColumnName("tuple_col", 1)

				if result[elem0Key] != 0 {
					t.Errorf("%s NULL tuple[0]: expected 0, got %v", method, result[elem0Key])
				}
				if result[elem1Key] != "" {
					t.Errorf("%s NULL tuple[1]: expected '', got %v", method, result[elem1Key])
				}
			})
		}
	})
}

// TestSliceMapMapScanVectorTypes tests vector types separately since they need Cassandra 5.0+ and special table setup
// (vectors need separate tables and version checks)
func TestSliceMapMapScanVectorTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Vector types require Cassandra 5.0+
	if session.control.getConn().host.Version().Before(5, 0, 0) {
		t.Skip("Vector types require Cassandra 5.0+")
	}

	// Create test table with vector columns
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_vector_test (
			id int PRIMARY KEY,
			vector_float_col vector<float, 3>,
			vector_text_col vector<text, 2>
		)
	`); err != nil {
		t.Fatal("Failed to create vector test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_vector_test").Exec(); err != nil {
		t.Fatal("Failed to truncate vector test table:", err)
	}

	testCases := []struct {
		colName       string
		cqlValue      string
		expectedValue interface{}
		expectedNull  interface{}
	}{
		{"vector_float_col", "[1.0, 2.5, -3.0]", []float32{1.0, 2.5, -3.0}, []float32(nil)},
		{"vector_text_col", "['hello', 'world']", []string{"hello", "world"}, []string(nil)},
	}

	for _, tc := range testCases {
		t.Run(tc.colName, func(t *testing.T) {
			// Test non-NULL value
			t.Run("NonNull", func(t *testing.T) {
				testID := 1
				// Insert non-NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_vector_test (id, %s) VALUES (?, %s)", tc.colName, tc.cqlValue)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert non-NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_vector_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						validateResult(t, tc.colName, tc.expectedValue, result, method, "non-NULL")
					})
				}
			})

			// Test NULL value
			t.Run("Null", func(t *testing.T) {
				testID := 2
				// Insert NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_vector_test (id, %s) VALUES (?, NULL)", tc.colName)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_vector_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						// Vectors should return nil slices for NULL values for consistency
						validateResult(t, tc.colName, tc.expectedNull, result, method, "NULL")
					})
				}
			})
		})
	}
}

// TestSliceMapMapScanCollectionTypes tests collection types separately since they have special handling
// (collections should return nil slices/maps for NULL values for consistency with other slice-based types)
func TestSliceMapMapScanCollectionTypes(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Create test table with collection columns
	if err := createTable(session, `
		CREATE TABLE IF NOT EXISTS gocql_test.slicemap_collection_test (
			id int PRIMARY KEY,
			list_col list<text>,
			set_col set<int>,
			map_col map<text, int>
		)
	`); err != nil {
		t.Fatal("Failed to create collection test table:", err)
	}

	// Clear existing data
	if err := session.Query("TRUNCATE gocql_test.slicemap_collection_test").Exec(); err != nil {
		t.Fatal("Failed to truncate collection test table:", err)
	}

	testCases := []struct {
		colName       string
		cqlValue      string
		expectedValue interface{}
		expectedNull  interface{}
	}{
		{"list_col", "['a', 'b', 'c']", []string{"a", "b", "c"}, []string(nil)},
		{"set_col", "{1, 2, 3}", []int{1, 2, 3}, []int(nil)},
		{"map_col", "{'key1': 1, 'key2': 2}", map[string]int{"key1": 1, "key2": 2}, map[string]int(nil)},
	}

	for _, tc := range testCases {
		t.Run(tc.colName, func(t *testing.T) {
			// Test non-NULL value
			t.Run("NonNull", func(t *testing.T) {
				testID := 1
				// Insert non-NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_collection_test (id, %s) VALUES (?, %s)", tc.colName, tc.cqlValue)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert non-NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_collection_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						// For sets, we need special comparison since order is not guaranteed
						if strings.HasPrefix(tc.colName, "set_") {
							if !compareCollectionValues(t, tc.colName, tc.expectedValue, result) {
								t.Errorf("%s non-NULL %s: expected %v, got %v", method, tc.colName, tc.expectedValue, result)
							}
						} else {
							validateResult(t, tc.colName, tc.expectedValue, result, method, "non-NULL")
						}
					})
				}
			})

			// Test NULL value
			t.Run("Null", func(t *testing.T) {
				testID := 2
				// Insert NULL value
				insertQuery := fmt.Sprintf("INSERT INTO gocql_test.slicemap_collection_test (id, %s) VALUES (?, NULL)", tc.colName)
				if err := session.Query(insertQuery, testID).Exec(); err != nil {
					t.Fatalf("Failed to insert NULL value: %v", err)
				}

				// Test both SliceMap and MapScan
				for _, method := range []string{"SliceMap", "MapScan"} {
					t.Run(method, func(t *testing.T) {
						var result interface{}

						selectQuery := fmt.Sprintf("SELECT %s FROM gocql_test.slicemap_collection_test WHERE id = ?", tc.colName)
						if method == "SliceMap" {
							iter := session.Query(selectQuery, testID).Iter()
							sliceResults, err := iter.SliceMap()
							iter.Close()
							if err != nil {
								t.Fatalf("SliceMap failed: %v", err)
							}
							if len(sliceResults) != 1 {
								t.Fatalf("Expected 1 result, got %d", len(sliceResults))
							}
							result = sliceResults[0][tc.colName]
						} else {
							mapResult := make(map[string]interface{})
							if err := session.Query(selectQuery, testID).MapScan(mapResult); err != nil {
								t.Fatalf("MapScan failed: %v", err)
							}
							result = mapResult[tc.colName]
						}

						// Collections should return nil slices/maps for NULL values for consistency
						validateResult(t, tc.colName, tc.expectedNull, result, method, "NULL")
					})
				}
			})
		})
	}
}
