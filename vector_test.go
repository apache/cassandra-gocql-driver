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
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
	"net"
	"reflect"
	"testing"
	"time"
)

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

	time1, _ := time.Parse("15:04:05", "01:00:00")
	time2, _ := time.Parse("15:04:05", "15:23:59")
	time3, _ := time.Parse("15:04:05.000", "10:31:45.987")

	duration1 := Duration{0, 1, 1920000000000}
	duration2 := Duration{1, 1, 1920000000000}
	duration3 := Duration{31, 0, 60000000000}

	tests := []struct {
		name       string
		cqlType    Type
		value      interface{}
		comparator func(interface{}, interface{})
	}{
		{name: "ascii", cqlType: TypeAscii, value: []string{"a", "1", "Z"}},
		// TODO(lantonia): Test vector of custom types
		// TODO(lantonia): Test vector of list, maps, set types
		{name: "bigint", cqlType: TypeBigInt, value: []int64{1, 2, 3}},
		{name: "blob", cqlType: TypeBlob, value: [][]byte{[]byte{1, 2, 3}, []byte{4, 5, 6, 7}, []byte{8, 9}}},
		{name: "boolean", cqlType: TypeBoolean, value: []bool{true, false, true}},
		{name: "counter", cqlType: TypeCounter, value: []int64{5, 6, 7}},
		{name: "decimal", cqlType: TypeDecimal, value: []inf.Dec{*inf.NewDec(1, 0), *inf.NewDec(2, 1), *inf.NewDec(-3, 2)}},
		{name: "double", cqlType: TypeDouble, value: []float64{0.1, -1.2, 3}},
		{name: "float", cqlType: TypeFloat, value: []float32{0.1, -1.2, 3}},
		{name: "int", cqlType: TypeInt, value: []int32{1, 2, 3}},
		{name: "text", cqlType: TypeText, value: []string{"a", "b", "c"}},
		{name: "timestamp", cqlType: TypeTimestamp, value: []time.Time{timestamp1, timestamp2, timestamp3}},
		{name: "uuid", cqlType: TypeUUID, value: []UUID{MustRandomUUID(), MustRandomUUID(), MustRandomUUID()}},
		{name: "varchar", cqlType: TypeVarchar, value: []string{"abc", "def", "ghi"}},
		{name: "varint", cqlType: TypeVarint, value: []uint64{uint64(1234), uint64(123498765), uint64(18446744073709551615)}},
		{name: "timeuuid", cqlType: TypeTimeUUID, value: []UUID{TimeUUID(), TimeUUID(), TimeUUID()}},
		{
			name:    "inet",
			cqlType: TypeInet,
			value:   []net.IP{net.IPv4(127, 0, 0, 1), net.IPv4(192, 168, 1, 1), net.IPv4(8, 8, 8, 8)},
			comparator: func(e interface{}, a interface{}) {
				expected := e.([]net.IP)
				actual := a.([]net.IP)
				assertEqual(t, "vector size", len(expected), len(actual))
				for i, _ := range expected {
					// TODO(lantoniak): Find a better way to compare IP addresses
					assertEqual(t, "vector", expected[i].String(), actual[i].String())
				}
			},
		},
		{name: "date", cqlType: TypeDate, value: []time.Time{date1, date2, date3}},
		{name: "time", cqlType: TypeTimestamp, value: []time.Time{time1, time2, time3}},
		{name: "smallint", cqlType: TypeSmallInt, value: []int16{127, 256, -1234}},
		{name: "tinyint", cqlType: TypeTinyInt, value: []int8{127, 9, -123}},
		{name: "duration", cqlType: TypeDuration, value: []Duration{duration1, duration2, duration3}},
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
