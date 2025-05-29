// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"strconv"
	"testing"
)

// Tests V1 and V2 metadata "compilation" from example data which might be returned
// from metadata schema queries (see getKeyspaceMetadata, getTableMetadata, and getColumnMetadata)
func TestCompileMetadata(t *testing.T) {
	// V1 tests - these are all based on real examples from the integration test ccm cluster
	log := &defaultLogger{}
	// V2 test - V2+ protocol is simpler so here are some toy examples to verify that the mapping works
	keyspace := &KeyspaceMetadata{
		Name: "V2Keyspace",
	}
	tables := []TableMetadata{
		{
			Keyspace: "V2Keyspace",
			Name:     "Table1",
		},
		{
			Keyspace: "V2Keyspace",
			Name:     "Table2",
		},
	}
	columns := []ColumnMetadata{
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table1",
			Name:           "KEY1",
			Kind:           ColumnPartitionKey,
			ComponentIndex: 0,
			Validator:      "org.apache.cassandra.db.marshal.UTF8Type",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table1",
			Name:           "Key1",
			Kind:           ColumnPartitionKey,
			ComponentIndex: 0,
			Validator:      "org.apache.cassandra.db.marshal.UTF8Type",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table2",
			Name:           "Column1",
			Kind:           ColumnPartitionKey,
			ComponentIndex: 0,
			Validator:      "org.apache.cassandra.db.marshal.UTF8Type",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table2",
			Name:           "Column2",
			Kind:           ColumnClusteringKey,
			ComponentIndex: 0,
			Validator:      "org.apache.cassandra.db.marshal.UTF8Type",
		},
		{
			Keyspace:       "V2Keyspace",
			Table:          "Table2",
			Name:           "Column3",
			Kind:           ColumnClusteringKey,
			ComponentIndex: 1,
			Validator:      "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)",
		},
		{
			Keyspace:  "V2Keyspace",
			Table:     "Table2",
			Name:      "Column4",
			Kind:      ColumnRegular,
			Validator: "org.apache.cassandra.db.marshal.UTF8Type",
		},
	}
	compileMetadata(2, keyspace, tables, columns, nil, nil, nil, nil, log)
	assertKeyspaceMetadata(
		t,
		keyspace,
		&KeyspaceMetadata{
			Name: "V2Keyspace",
			Tables: map[string]*TableMetadata{
				"Table1": {
					PartitionKey: []*ColumnMetadata{
						{
							Name: "Key1",
							Type: NativeType{typ: TypeVarchar},
						},
					},
					ClusteringColumns: []*ColumnMetadata{},
					Columns: map[string]*ColumnMetadata{
						"KEY1": {
							Name: "KEY1",
							Type: NativeType{typ: TypeVarchar},
							Kind: ColumnPartitionKey,
						},
						"Key1": {
							Name: "Key1",
							Type: NativeType{typ: TypeVarchar},
							Kind: ColumnPartitionKey,
						},
					},
				},
				"Table2": {
					PartitionKey: []*ColumnMetadata{
						{
							Name: "Column1",
							Type: NativeType{typ: TypeVarchar},
						},
					},
					ClusteringColumns: []*ColumnMetadata{
						{
							Name:  "Column2",
							Type:  NativeType{typ: TypeVarchar},
							Order: ASC,
						},
						{
							Name:  "Column3",
							Type:  NativeType{typ: TypeVarchar},
							Order: DESC,
						},
					},
					Columns: map[string]*ColumnMetadata{
						"Column1": {
							Name: "Column1",
							Type: NativeType{typ: TypeVarchar},
							Kind: ColumnPartitionKey,
						},
						"Column2": {
							Name:  "Column2",
							Type:  NativeType{typ: TypeVarchar},
							Order: ASC,
							Kind:  ColumnClusteringKey,
						},
						"Column3": {
							Name:  "Column3",
							Type:  NativeType{typ: TypeVarchar},
							Order: DESC,
							Kind:  ColumnClusteringKey,
						},
						"Column4": {
							Name: "Column4",
							Type: NativeType{typ: TypeVarchar},
							Kind: ColumnRegular,
						},
					},
				},
			},
		},
	)
}

// Helper function for asserting that actual metadata returned was as expected
func assertKeyspaceMetadata(t *testing.T, actual, expected *KeyspaceMetadata) {
	if len(expected.Tables) != len(actual.Tables) {
		t.Errorf("Expected len(%s.Tables) to be %v but was %v", expected.Name, len(expected.Tables), len(actual.Tables))
	}
	for keyT := range expected.Tables {
		et := expected.Tables[keyT]
		at, found := actual.Tables[keyT]

		if !found {
			t.Errorf("Expected %s.Tables[%s] but was not found", expected.Name, keyT)
		} else {
			if keyT != at.Name {
				t.Errorf("Expected %s.Tables[%s].Name to be %v but was %v", expected.Name, keyT, keyT, at.Name)
			}
			if len(et.PartitionKey) != len(at.PartitionKey) {
				t.Errorf("Expected len(%s.Tables[%s].PartitionKey) to be %v but was %v", expected.Name, keyT, len(et.PartitionKey), len(at.PartitionKey))
			} else {
				for i := range et.PartitionKey {
					if et.PartitionKey[i].Name != at.PartitionKey[i].Name {
						t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Name to be '%v' but was '%v'", expected.Name, keyT, i, et.PartitionKey[i].Name, at.PartitionKey[i].Name)
					}
					if expected.Name != at.PartitionKey[i].Keyspace {
						t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Keyspace to be '%v' but was '%v'", expected.Name, keyT, i, expected.Name, at.PartitionKey[i].Keyspace)
					}
					if keyT != at.PartitionKey[i].Table {
						t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Table to be '%v' but was '%v'", expected.Name, keyT, i, keyT, at.PartitionKey[i].Table)
					}
					if et.PartitionKey[i].Type.Type() != at.PartitionKey[i].Type.Type() {
						t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Type.Type to be %v but was %v", expected.Name, keyT, i, et.PartitionKey[i].Type.Type(), at.PartitionKey[i].Type.Type())
					}
					if i != at.PartitionKey[i].ComponentIndex {
						t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].ComponentIndex to be %v but was %v", expected.Name, keyT, i, i, at.PartitionKey[i].ComponentIndex)
					}
					if ColumnPartitionKey != at.PartitionKey[i].Kind {
						t.Errorf("Expected %s.Tables[%s].PartitionKey[%d].Kind to be '%v' but was '%v'", expected.Name, keyT, i, ColumnPartitionKey, at.PartitionKey[i].Kind)
					}
				}
			}
			if len(et.ClusteringColumns) != len(at.ClusteringColumns) {
				t.Errorf("Expected len(%s.Tables[%s].ClusteringColumns) to be %v but was %v", expected.Name, keyT, len(et.ClusteringColumns), len(at.ClusteringColumns))
			} else {
				for i := range et.ClusteringColumns {
					if at.ClusteringColumns[i] == nil {
						t.Fatalf("Unexpected nil value: %s.Tables[%s].ClusteringColumns[%d]", expected.Name, keyT, i)
					}
					if et.ClusteringColumns[i].Name != at.ClusteringColumns[i].Name {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Name to be '%v' but was '%v'", expected.Name, keyT, i, et.ClusteringColumns[i].Name, at.ClusteringColumns[i].Name)
					}
					if expected.Name != at.ClusteringColumns[i].Keyspace {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Keyspace to be '%v' but was '%v'", expected.Name, keyT, i, expected.Name, at.ClusteringColumns[i].Keyspace)
					}
					if keyT != at.ClusteringColumns[i].Table {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Table to be '%v' but was '%v'", expected.Name, keyT, i, keyT, at.ClusteringColumns[i].Table)
					}
					if et.ClusteringColumns[i].Type.Type() != at.ClusteringColumns[i].Type.Type() {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Type.Type to be %v but was %v", expected.Name, keyT, i, et.ClusteringColumns[i].Type.Type(), at.ClusteringColumns[i].Type.Type())
					}
					if i != at.ClusteringColumns[i].ComponentIndex {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].ComponentIndex to be %v but was %v", expected.Name, keyT, i, i, at.ClusteringColumns[i].ComponentIndex)
					}
					if et.ClusteringColumns[i].Order != at.ClusteringColumns[i].Order {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Order to be %v but was %v", expected.Name, keyT, i, et.ClusteringColumns[i].Order, at.ClusteringColumns[i].Order)
					}
					if ColumnClusteringKey != at.ClusteringColumns[i].Kind {
						t.Errorf("Expected %s.Tables[%s].ClusteringColumns[%d].Kind to be '%v' but was '%v'", expected.Name, keyT, i, ColumnClusteringKey, at.ClusteringColumns[i].Kind)
					}
				}
			}
			if len(et.Columns) != len(at.Columns) {
				eKeys := make([]string, 0, len(et.Columns))
				for key := range et.Columns {
					eKeys = append(eKeys, key)
				}
				aKeys := make([]string, 0, len(at.Columns))
				for key := range at.Columns {
					aKeys = append(aKeys, key)
				}
				t.Errorf("Expected len(%s.Tables[%s].Columns) to be %v (keys:%v) but was %v (keys:%v)", expected.Name, keyT, len(et.Columns), eKeys, len(at.Columns), aKeys)
			} else {
				for keyC := range et.Columns {
					ec := et.Columns[keyC]
					ac, found := at.Columns[keyC]

					if !found {
						t.Errorf("Expected %s.Tables[%s].Columns[%s] but was not found", expected.Name, keyT, keyC)
					} else {
						if keyC != ac.Name {
							t.Errorf("Expected %s.Tables[%s].Columns[%s].Name to be '%v' but was '%v'", expected.Name, keyT, keyC, keyC, at.Name)
						}
						if expected.Name != ac.Keyspace {
							t.Errorf("Expected %s.Tables[%s].Columns[%s].Keyspace to be '%v' but was '%v'", expected.Name, keyT, keyC, expected.Name, ac.Keyspace)
						}
						if keyT != ac.Table {
							t.Errorf("Expected %s.Tables[%s].Columns[%s].Table to be '%v' but was '%v'", expected.Name, keyT, keyC, keyT, ac.Table)
						}
						if ec.Type.Type() != ac.Type.Type() {
							t.Errorf("Expected %s.Tables[%s].Columns[%s].Type.Type to be %v but was %v", expected.Name, keyT, keyC, ec.Type.Type(), ac.Type.Type())
						}
						if ec.Order != ac.Order {
							t.Errorf("Expected %s.Tables[%s].Columns[%s].Order to be %v but was %v", expected.Name, keyT, keyC, ec.Order, ac.Order)
						}
						if ec.Kind != ac.Kind {
							t.Errorf("Expected %s.Tables[%s].Columns[%s].Kind to be '%v' but was '%v'", expected.Name, keyT, keyC, ec.Kind, ac.Kind)
						}
					}
				}
			}
		}
	}
}

// Tests the cassandra type definition parser
func TestTypeParser(t *testing.T) {
	// native type
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.UTF8Type",
		assertTypeInfo{Type: TypeVarchar},
	)

	// reversed
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UUIDType)",
		assertTypeInfo{Type: TypeUUID, Reversed: true},
	)

	// set
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)",
		assertTypeInfo{
			Type: TypeSet,
			Elem: &assertTypeInfo{Type: TypeInt},
		},
	)

	// list
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TimeUUIDType)",
		assertTypeInfo{
			Type: TypeList,
			Elem: &assertTypeInfo{Type: TypeTimeUUID},
		},
	)

	// map
	assertParseNonCompositeType(
		t,
		" org.apache.cassandra.db.marshal.MapType( org.apache.cassandra.db.marshal.UUIDType , org.apache.cassandra.db.marshal.BytesType ) ",
		assertTypeInfo{
			Type: TypeMap,
			Key:  &assertTypeInfo{Type: TypeUUID},
			Elem: &assertTypeInfo{Type: TypeBlob},
		},
	)

	// udt
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.UserType(sandbox,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a6970:org.apache.cassandra.db.marshal.Int32Type)",
		assertTypeInfo{Type: TypeUDT, Custom: ""},
	)

	// custom
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.DynamicCompositeType(u=>org.apache.cassandra.db.marshal.UUIDType,d=>org.apache.cassandra.db.marshal.DateType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,b=>org.apache.cassandra.db.marshal.BytesType,s=>org.apache.cassandra.db.marshal.UTF8Type,B=>org.apache.cassandra.db.marshal.BooleanType,a=>org.apache.cassandra.db.marshal.AsciiType,l=>org.apache.cassandra.db.marshal.LongType,i=>org.apache.cassandra.db.marshal.IntegerType,x=>org.apache.cassandra.db.marshal.LexicalUUIDType)",
		assertTypeInfo{Type: TypeCustom, Custom: "org.apache.cassandra.db.marshal.DynamicCompositeType(u=>org.apache.cassandra.db.marshal.UUIDType,d=>org.apache.cassandra.db.marshal.DateType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,b=>org.apache.cassandra.db.marshal.BytesType,s=>org.apache.cassandra.db.marshal.UTF8Type,B=>org.apache.cassandra.db.marshal.BooleanType,a=>org.apache.cassandra.db.marshal.AsciiType,l=>org.apache.cassandra.db.marshal.LongType,i=>org.apache.cassandra.db.marshal.IntegerType,x=>org.apache.cassandra.db.marshal.LexicalUUIDType)"},
	)

	// composite defs
	assertParseCompositeType(
		t,
		"org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)",
		[]assertTypeInfo{
			{Type: TypeVarchar},
		},
		nil,
	)
	assertParseCompositeType(
		t,
		"org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.DateType),org.apache.cassandra.db.marshal.UTF8Type)",
		[]assertTypeInfo{
			{Type: TypeTimestamp, Reversed: true},
			{Type: TypeVarchar},
		},
		nil,
	)
	assertParseCompositeType(
		t,
		"org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(726f77735f6d6572676564:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.LongType)))",
		[]assertTypeInfo{
			{Type: TypeVarchar},
		},
		map[string]assertTypeInfo{
			"rows_merged": {
				Type: TypeMap,
				Key:  &assertTypeInfo{Type: TypeInt},
				Elem: &assertTypeInfo{Type: TypeBigInt},
			},
		},
	)
}

// expected data holder
type assertTypeInfo struct {
	Type     Type
	Reversed bool
	Elem     *assertTypeInfo
	Key      *assertTypeInfo
	Custom   string
}

// Helper function for asserting that the type parser returns the expected
// results for the given definition
func assertParseNonCompositeType(
	t *testing.T,
	def string,
	typeExpected assertTypeInfo,
) {

	log := &defaultLogger{}
	result := parseType(def, protoVersion4, log)
	if len(result.reversed) != 1 {
		t.Errorf("%s expected %d reversed values but there were %d", def, 1, len(result.reversed))
	}

	assertParseNonCompositeTypes(
		t,
		def,
		[]assertTypeInfo{typeExpected},
		result.types,
	)

	// expect no composite part of the result
	if result.isComposite {
		t.Errorf("%s: Expected not composite", def)
	}
	if result.collections != nil {
		t.Errorf("%s: Expected nil collections: %v", def, result.collections)
	}
}

// Helper function for asserting that the type parser returns the expected
// results for the given definition
func assertParseCompositeType(
	t *testing.T,
	def string,
	typesExpected []assertTypeInfo,
	collectionsExpected map[string]assertTypeInfo,
) {

	log := &defaultLogger{}
	result := parseType(def, protoVersion4, log)
	if len(result.reversed) != len(typesExpected) {
		t.Errorf("%s expected %d reversed values but there were %d", def, len(typesExpected), len(result.reversed))
	}

	assertParseNonCompositeTypes(
		t,
		def,
		typesExpected,
		result.types,
	)

	// expect composite part of the result
	if !result.isComposite {
		t.Errorf("%s: Expected composite", def)
	}
	if result.collections == nil && collectionsExpected != nil {
		t.Errorf("%s: Expected non-nil collections: %v", def, result.collections)
	}

	for name, typeExpected := range collectionsExpected {
		// check for an actual type for this name
		typeActual, found := result.collections[name]
		if !found {
			t.Errorf("%s.tcollections: Expected param named %s but there wasn't", def, name)
		} else {
			// remove the actual from the collection so we can detect extras
			delete(result.collections, name)

			// check the type
			assertParseNonCompositeTypes(
				t,
				def+"collections["+name+"]",
				[]assertTypeInfo{typeExpected},
				[]TypeInfo{typeActual},
			)
		}
	}

	if len(result.collections) != 0 {
		t.Errorf("%s.collections: Expected no more types in collections, but there was %v", def, result.collections)
	}
}

// Helper function for asserting that the type parser returns the expected
// results for the given definition
func assertParseNonCompositeTypes(
	t *testing.T,
	context string,
	typesExpected []assertTypeInfo,
	typesActual []TypeInfo,
) {
	if len(typesActual) != len(typesExpected) {
		t.Errorf("%s: Expected %d types, but there were %d", context, len(typesExpected), len(typesActual))
	}

	for i := range typesExpected {
		typeExpected := typesExpected[i]
		typeActual := typesActual[i]

		// shadow copy the context for local modification
		context := context
		if len(typesExpected) > 1 {
			context = context + "[" + strconv.Itoa(i) + "]"
		}

		// check the type
		if typeActual.Type() != typeExpected.Type {
			t.Errorf("%s: Expected to parse Type to %s but was %s", context, typeExpected.Type, typeActual.Type())
		}
		// check the custom
		if typeActual.Custom() != typeExpected.Custom {
			t.Errorf("%s: Expected to parse Custom %s but was %s", context, typeExpected.Custom, typeActual.Custom())
		}

		collection, _ := typeActual.(CollectionType)
		// check the elem
		if typeExpected.Elem != nil {
			if collection.Elem == nil {
				t.Errorf("%s: Expected to parse Elem, but was nil ", context)
			} else {
				assertParseNonCompositeTypes(
					t,
					context+".Elem",
					[]assertTypeInfo{*typeExpected.Elem},
					[]TypeInfo{collection.Elem},
				)
			}
		} else if collection.Elem != nil {
			t.Errorf("%s: Expected to not parse Elem, but was %+v", context, collection.Elem)
		}

		// check the key
		if typeExpected.Key != nil {
			if collection.Key == nil {
				t.Errorf("%s: Expected to parse Key, but was nil ", context)
			} else {
				assertParseNonCompositeTypes(
					t,
					context+".Key",
					[]assertTypeInfo{*typeExpected.Key},
					[]TypeInfo{collection.Key},
				)
			}
		} else if collection.Key != nil {
			t.Errorf("%s: Expected to not parse Key, but was %+v", context, collection.Key)
		}
	}
}
