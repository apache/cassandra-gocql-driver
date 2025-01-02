//go:build all || cassandra
// +build all cassandra

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

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
)

type person struct {
	FirstName string `cql:"first_name"`
	LastName  string `cql:"last_name"`
	Age       int    `cql:"age"`
}

func (p person) String() string {
	return fmt.Sprintf("Person{firstName: %s, lastName: %s, Age: %d}", p.FirstName, p.LastName, p.Age)
}

func TestVector_Marshaler(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_fixed(id int primary key, vec vector<float, 3>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_variable(id int primary key, vec vector<text, 4>);`)
	if err != nil {
		t.Fatal(err)
	}

	insertFixVec := []float32{8, 2.5, -5.0}
	err = session.Query("INSERT INTO vector_fixed(id, vec) VALUES(?, ?)", 1, insertFixVec).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var selectFixVec []float32
	err = session.Query("SELECT vec FROM vector_fixed WHERE id = ?", 1).Scan(&selectFixVec)
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "fixed size element vector", insertFixVec, selectFixVec)

	longText := randomText(500)
	insertVarVec := []string{"apache", "cassandra", longText, "gocql"}
	err = session.Query("INSERT INTO vector_variable(id, vec) VALUES(?, ?)", 1, insertVarVec).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var selectVarVec []string
	err = session.Query("SELECT vec FROM vector_variable WHERE id = ?", 1).Scan(&selectVarVec)
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "variable size element vector", insertVarVec, selectVarVec)
}

func TestVector_Types(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	timestamp1, _ := time.Parse("2006-01-02", "2000-01-01")
	timestamp2, _ := time.Parse("2006-01-02 15:04:05", "2024-01-01 10:31:45")
	timestamp3, _ := time.Parse("2006-01-02 15:04:05.000", "2024-05-01 10:31:45.987")

	date1, _ := time.Parse("2006-01-02", "2000-01-01")
	date2, _ := time.Parse("2006-01-02", "2022-03-14")
	date3, _ := time.Parse("2006-01-02", "2024-12-31")

	time1 := time.Duration(time.Hour)
	time2 := time.Duration(15*time.Hour + 23*time.Minute + 59*time.Second)
	time3 := time.Duration(10*time.Hour + 31*time.Minute + 45*time.Second + 987*time.Millisecond)

	duration1 := Duration{0, 1, 1920000000000}
	duration2 := Duration{1, 1, 1920000000000}
	duration3 := Duration{31, 0, 60000000000}

	map1 := make(map[string]int)
	map1["a"] = 1
	map1["b"] = 2
	map1["c"] = 3
	map2 := make(map[string]int)
	map2["abc"] = 123
	map3 := make(map[string]int)

	tests := []struct {
		name       string
		cqlType    string
		value      interface{}
		comparator func(interface{}, interface{})
	}{
		{name: "ascii", cqlType: "ascii", value: []string{"a", "1", "Z"}},
		{name: "bigint", cqlType: "bigint", value: []int64{1, 2, 3}},
		{name: "blob", cqlType: "blob", value: [][]byte{[]byte{1, 2, 3}, []byte{4, 5, 6, 7}, []byte{8, 9}}},
		{name: "boolean", cqlType: "boolean", value: []bool{true, false, true}},
		{name: "counter", cqlType: "counter", value: []int64{5, 6, 7}},
		{name: "decimal", cqlType: "decimal", value: []inf.Dec{*inf.NewDec(1, 0), *inf.NewDec(2, 1), *inf.NewDec(-3, 2)}},
		{name: "double", cqlType: "double", value: []float64{0.1, -1.2, 3}},
		{name: "float", cqlType: "float", value: []float32{0.1, -1.2, 3}},
		{name: "int", cqlType: "int", value: []int32{1, 2, 3}},
		{name: "text", cqlType: "text", value: []string{"a", "b", "c"}},
		{name: "timestamp", cqlType: "timestamp", value: []time.Time{timestamp1, timestamp2, timestamp3}},
		{name: "uuid", cqlType: "uuid", value: []UUID{MustRandomUUID(), MustRandomUUID(), MustRandomUUID()}},
		{name: "varchar", cqlType: "varchar", value: []string{"abc", "def", "ghi"}},
		{name: "varint", cqlType: "varint", value: []uint64{uint64(1234), uint64(123498765), uint64(18446744073709551615)}},
		{name: "timeuuid", cqlType: "timeuuid", value: []UUID{TimeUUID(), TimeUUID(), TimeUUID()}},
		{
			name:    "inet",
			cqlType: "inet",
			value:   []net.IP{net.IPv4(127, 0, 0, 1), net.IPv4(192, 168, 1, 1), net.IPv4(8, 8, 8, 8)},
			comparator: func(e interface{}, a interface{}) {
				expected := e.([]net.IP)
				actual := a.([]net.IP)
				assertEqual(t, "vector size", len(expected), len(actual))
				for i, _ := range expected {
					assertTrue(t, "vector", expected[i].Equal(actual[i]))
				}
			},
		},
		{name: "date", cqlType: "date", value: []time.Time{date1, date2, date3}},
		{name: "time", cqlType: "time", value: []time.Duration{time1, time2, time3}},
		{name: "smallint", cqlType: "smallint", value: []int16{127, 256, -1234}},
		{name: "tinyint", cqlType: "tinyint", value: []int8{127, 9, -123}},
		{name: "duration", cqlType: "duration", value: []Duration{duration1, duration2, duration3}},
		{name: "vector_vector_float", cqlType: "vector<float, 5>", value: [][]float32{{0.1, -1.2, 3, 5, 5}, {10.1, -122222.0002, 35.0, 1, 1}, {0, 0, 0, 0, 0}}},
		{name: "vector_vector_set_float", cqlType: "vector<set<float>, 5>", value: [][][]float32{
			{{1, 2}, {2, -1}, {3}, {0}, {-1.3}},
			{{2, 3}, {2, -1}, {3}, {0}, {-1.3}},
			{{1, 1000.0}, {0}, {}, {12, 14, 15, 16}, {-1.3}},
		}},
		{name: "vector_tuple_text_int_float", cqlType: "tuple<text, int, float>", value: [][]interface{}{{"a", 1, float32(0.5)}, {"b", 2, float32(-1.2)}, {"c", 3, float32(0)}}},
		{name: "vector_tuple_text_list_text", cqlType: "tuple<text, list<text>>", value: [][]interface{}{{"a", []string{"b", "c"}}, {"d", []string{"e", "f", "g"}}, {"h", []string{"i"}}}},
		{name: "vector_set_text", cqlType: "set<text>", value: [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}}},
		{name: "vector_list_int", cqlType: "list<int>", value: [][]int32{{1, 2, 3}, {-1, -2, -3}, {0, 0, 0}}},
		{name: "vector_map_text_int", cqlType: "map<text, int>", value: []map[string]int{map1, map2, map3}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableName := fmt.Sprintf("vector_%s", test.name)
			err := createTable(session, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS gocql_test.%s(id int primary key, vec vector<%s, 3>);`, tableName, test.cqlType))
			if err != nil {
				t.Fatal(err)
			}

			err = session.Query(fmt.Sprintf("INSERT INTO %s(id, vec) VALUES(?, ?)", tableName), 1, test.value).Exec()
			if err != nil {
				t.Fatal(err)
			}

			v := reflect.New(reflect.TypeOf(test.value))
			err = session.Query(fmt.Sprintf("SELECT vec FROM %s WHERE id = ?", tableName), 1).Scan(v.Interface())
			if err != nil {
				t.Fatal(err)
			}
			if test.comparator != nil {
				test.comparator(test.value, v.Elem().Interface())
			} else {
				assertDeepEqual(t, "vector", test.value, v.Elem().Interface())
			}
		})
	}
}

func TestVector_MarshalerUDT(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TYPE gocql_test.person(
		first_name text,
		last_name text,
		age int);`)
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, `CREATE TABLE gocql_test.vector_relatives(
		id int,
		couple vector<person, 2>,
		primary key(id)
	);`)
	if err != nil {
		t.Fatal(err)
	}

	p1 := person{"Johny", "Bravo", 25}
	p2 := person{"Capitan", "Planet", 5}
	insVec := []person{p1, p2}

	err = session.Query("INSERT INTO vector_relatives(id, couple) VALUES(?, ?)", 1, insVec).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var selVec []person

	err = session.Query("SELECT couple FROM vector_relatives WHERE id = ?", 1).Scan(&selVec)
	if err != nil {
		t.Fatal(err)
	}

	assertDeepEqual(t, "udt", &insVec, &selVec)
}

func TestVector_Empty(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_fixed_null(id int primary key, vec vector<float, 3>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_variable_null(id int primary key, vec vector<text, 4>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = session.Query("INSERT INTO vector_fixed_null(id) VALUES(?)", 1).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var selectFixVec []float32
	err = session.Query("SELECT vec FROM vector_fixed_null WHERE id = ?", 1).Scan(&selectFixVec)
	if err != nil {
		t.Fatal(err)
	}
	assertTrue(t, "fixed size element vector is empty", selectFixVec == nil)

	err = session.Query("INSERT INTO vector_variable_null(id) VALUES(?)", 1).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var selectVarVec []string
	err = session.Query("SELECT vec FROM vector_variable_null WHERE id = ?", 1).Scan(&selectVarVec)
	if err != nil {
		t.Fatal(err)
	}
	assertTrue(t, "variable size element vector is empty", selectVarVec == nil)
}

func TestVector_MissingDimension(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_fixed(id int primary key, vec vector<float, 3>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = session.Query("INSERT INTO vector_fixed(id, vec) VALUES(?, ?)", 1, []float32{8, -5.0}).Exec()
	require.Error(t, err, "expected vector with 3 dimensions, received 2")

	err = session.Query("INSERT INTO vector_fixed(id, vec) VALUES(?, ?)", 1, []float32{8, -5.0, 1, 3}).Exec()
	require.Error(t, err, "expected vector with 3 dimensions, received 4")
}

func TestReadUnsignedVInt(t *testing.T) {
	tests := []struct {
		decodedInt  uint64
		encodedVint []byte
	}{
		{
			decodedInt:  0,
			encodedVint: []byte{0},
		},
		{
			decodedInt:  100,
			encodedVint: []byte{100},
		},
		{
			decodedInt:  256000,
			encodedVint: []byte{195, 232, 0},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test.decodedInt), func(t *testing.T) {
			actual, _, err := readUnsignedVInt(test.encodedVint)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if actual != test.decodedInt {
				t.Fatalf("Expected %d, but got %d", test.decodedInt, actual)
			}
		})
	}
}
