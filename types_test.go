//go:build all || unit
// +build all unit

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
	"reflect"
	"testing"
)

var defaultLongTypes = []struct {
	TypeName string
}{
	{"AsciiType"},
	{"LongType"},
	{"BytesType"},
	{"BooleanType"},
	{"CounterColumnType"},
	{"DecimalType"},
	{"DoubleType"},
	{"FloatType"},
	{"Int32Type"},
	{"DateType"},
	{"TimestampType"},
	{"UUIDType"},
	{"UTF8Type"},
	{"IntegerType"},
	{"TimeUUIDType"},
	{"InetAddressType"},
	{"MapType"},
	{"ListType"},
	{"SetType"},
	{"ShortType"},
	{"ByteType"},
	{"TupleType"},
	{"UserType"},
	{"VectorType"},
}

func testType(t *testing.T, str string) {
	_, ok := GlobalTypes.getType(apacheCassandraTypePrefix + str)
	if !ok {
		t.Errorf("failed to get type for %v", apacheCassandraTypePrefix+str)
	}
}

func TestDefaultLongTypes(t *testing.T) {
	for _, lookupTest := range defaultLongTypes {
		testType(t, lookupTest.TypeName)
	}
}

func TestSplitCompositeTypes(t *testing.T) {
	var testCases = []struct {
		Name  string
		Split []string
	}{
		{
			Name:  "boolean",
			Split: []string{"boolean"},
		},
		{
			Name:  "boolean, int",
			Split: []string{"boolean", "int"},
		},
		{
			Name:  apacheCassandraTypePrefix + "TupleType(a, b)",
			Split: []string{apacheCassandraTypePrefix + "TupleType(a, b)"},
		},
		{
			Name:  "tuple<a,b>",
			Split: []string{"tuple<a,b>"},
		},
		{
			Name:  "tuple<tuple<a>,b>",
			Split: []string{"tuple<tuple<a>,b>"},
		},
		{
			Name:  "tuple<tuple<a>,b>, int",
			Split: []string{"tuple<tuple<a>,b>", "int"},
		},
		{
			Name: apacheCassandraTypePrefix + "TupleType(a, b), " + apacheCassandraTypePrefix + "IntType",
			Split: []string{
				apacheCassandraTypePrefix + "TupleType(a, b)",
				apacheCassandraTypePrefix + "IntType",
			},
		},
	}
	for _, tc := range testCases {
		split := splitCompositeTypes(tc.Name)
		if !reflect.DeepEqual(split, tc.Split) {
			t.Errorf("[%v] expected %v, got %v", tc.Name, tc.Split, split)
		}
	}
}
