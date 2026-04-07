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

func TestRegisteredTypes_CopyRebindsCodecs(t *testing.T) {
	const listOfUDT = "org.apache.cassandra.db.marshal.ListType(" +
		"org.apache.cassandra.db.marshal.UserType(ks,6e616d65,66:org.apache.cassandra.db.marshal.Int32Type))"

	nestedUDTFlag := func(t *testing.T, rt *RegisteredTypes) bool {
		t.Helper()
		ti, err := rt.typeInfoFromString(protoVersion4, listOfUDT)
		if err != nil {
			t.Fatalf("typeInfoFromString: %v", err)
		}
		list, ok := ti.(CollectionType)
		if !ok {
			t.Fatalf("expected CollectionType, got %T", ti)
		}
		udt, ok := list.Elem.(UDTTypeInfo)
		if !ok {
			t.Fatalf("expected UDTTypeInfo element, got %T", list.Elem)
		}
		return udt.EncodeNilMapAsNull
	}

	enabled := GlobalTypes.WithNilMapAsNullUDT(true)
	disabled := enabled.WithNilMapAsNullUDT(false)

	if !nestedUDTFlag(t, enabled) {
		t.Error("nested UDT should see the flag set by WithNilMapAsNullUDT(true)")
	}
	if !nestedUDTFlag(t, enabled.Copy()) {
		t.Error("Copy must preserve the flag for nested UDTs")
	}
	if nestedUDTFlag(t, disabled) {
		t.Error("nested UDT should see the flag set by WithNilMapAsNullUDT(false)")
	}
	// creating a copy must not change the registry it was created from
	if !nestedUDTFlag(t, enabled) {
		t.Error("WithNilMapAsNullUDT(false) must not change the original")
	}
	if nestedUDTFlag(t, GlobalTypes) {
		t.Error("WithNilMapAsNullUDT must not change GlobalTypes")
	}
}

func TestRegisteredTypes_NullableUDTMaps(t *testing.T) {
	// readTypeInfo resolves types through the framer's registry, so the flag
	// must be visible for a UDT read from a frame, directly and when nested.
	writeUDT := func(f *framer) {
		f.writeShort(uint16(TypeUDT))
		f.writeString("ks")
		f.writeString("name")
		f.writeShort(1)
		f.writeString("f")
		f.writeShort(uint16(TypeInt))
	}
	udtFlag := func(t *testing.T, rt *RegisteredTypes, nested bool) bool {
		t.Helper()
		f := newFramer(nil, protoVersion4, rt)
		if nested {
			f.writeShort(uint16(TypeList))
		}
		writeUDT(f)
		ti, err := f.readTypeInfo()
		if err != nil {
			t.Fatalf("readTypeInfo: %v", err)
		}
		if nested {
			ti = ti.(CollectionType).Elem
		}
		udt, ok := ti.(UDTTypeInfo)
		if !ok {
			t.Fatalf("expected UDTTypeInfo, got %T", ti)
		}
		return udt.EncodeNilMapAsNull
	}

	enabled := GlobalTypes.WithNilMapAsNullUDT(true)
	for _, nested := range []bool{false, true} {
		if !udtFlag(t, enabled, nested) {
			t.Errorf("nested=%v: flag should be visible after WithNilMapAsNullUDT(true)", nested)
		}
		if !udtFlag(t, enabled.Copy(), nested) {
			t.Errorf("nested=%v: flag should survive Copy", nested)
		}
		if udtFlag(t, GlobalTypes, nested) {
			t.Errorf("nested=%v: GlobalTypes must keep the legacy default", nested)
		}
	}
}

func TestRegisteredTypes_CopyKeepsUserCodecs(t *testing.T) {
	// A registry that does not start from GlobalTypes can hold the user's own
	// codecs for composite types. Copy, which every session goes through, must
	// keep them and must not add built-in codecs the source did not have.
	custom := SimpleCQLType{intTypeInfo{}}
	src := &RegisteredTypes{}
	if err := src.RegisterType(TypeList, "list", custom); err != nil {
		t.Fatal(err)
	}

	cp := src.Copy()
	if got := cp.byType[TypeList]; got != CQLType(custom) {
		t.Errorf("Copy replaced the user's codec for list: got %#v", got)
	}
	if _, ok := cp.byType[TypeMap]; ok {
		t.Error("Copy must not add built-in codecs that the source did not have")
	}
}
