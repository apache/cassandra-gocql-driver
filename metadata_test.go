//go:build all || unit
// +build all unit

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

	"github.com/stretchr/testify/require"
)

// Tests V1 and V2 metadata "compilation" from example data which might be returned
// from metadata schema queries (see getKeyspaceMetadata, getTableMetadata, and getColumnMetadata)
func TestCompileMetadata(t *testing.T) {
	session := &Session{
		cfg: ClusterConfig{
			ProtoVersion: 1,
		},
		logger: NewLogger(LogLevelNone),
		types:  GlobalTypes,
	}
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
	compileMetadata(session, keyspace, tables, columns, nil, nil, nil, nil)
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
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
						},
					},
					ClusteringColumns: []*ColumnMetadata{},
					Columns: map[string]*ColumnMetadata{
						"KEY1": {
							Name: "KEY1",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Kind: ColumnPartitionKey,
						},
						"Key1": {
							Name: "Key1",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Kind: ColumnPartitionKey,
						},
					},
				},
				"Table2": {
					PartitionKey: []*ColumnMetadata{
						{
							Name: "Column1",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
						},
					},
					ClusteringColumns: []*ColumnMetadata{
						{
							Name: "Column2",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Order: ASC,
						},
						{
							Name: "Column3",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Order: DESC,
						},
					},
					Columns: map[string]*ColumnMetadata{
						"Column1": {
							Name: "Column1",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Kind: ColumnPartitionKey,
						},
						"Column2": {
							Name: "Column2",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Order: ASC,
							Kind:  ColumnClusteringKey,
						},
						"Column3": {
							Name: "Column3",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
							Order: DESC,
							Kind:  ColumnClusteringKey,
						},
						"Column4": {
							Name: "Column4",
							Type: varcharLikeTypeInfo{
								typ: TypeVarchar,
							},
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

	session := &Session{
		cfg: ClusterConfig{
			ProtoVersion: 4,
		},
		logger: NewLogger(LogLevelNone),
		types:  GlobalTypes,
	}
	result, err := parseType(session, def)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.reversed) != 1 {
		t.Errorf("%s expected %d reversed values but there were %d", def, 1, len(result.reversed))
		return
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

	session := &Session{
		cfg: ClusterConfig{
			ProtoVersion: 4,
		},
		logger: NewLogger(LogLevelNone),
		types:  GlobalTypes,
	}
	result, err := parseType(session, def)
	if err != nil {
		t.Fatal(err)
	}
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
			t.Errorf("%s: Expected to parse Type to %v but was %v", context, typeExpected.Type, typeActual.Type())
		}
		if typeExpected.Custom != "" {
			ct, ok := typeActual.(unknownTypeInfo)
			if !ok {
				t.Errorf("%s: Expected to get unknownCustomTypeInfo but was %T", context, typeActual)
				continue
			}
			if string(ct) != typeExpected.Custom {
				t.Errorf("%s: Expected to parse Custom %s but was %s", context, typeExpected.Custom, string(ct))
			}
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

func TestCompileMetadataWithFunctions(t *testing.T) {
	session := &Session{
		cfg: ClusterConfig{
			ProtoVersion: protoVersion5,
			Logger:       NewLogger(LogLevelInfo),
		},
		types: GlobalTypes.Copy(),
	}

	keyspace := &KeyspaceMetadata{
		Name: "test_keyspace",
	}

	functions := []FunctionMetadata{
		{
			Keyspace:          "test_keyspace",
			Name:              "test_func",
			ArgumentTypes:     []TypeInfo{intTypeInfo{}},
			ArgumentNames:     []string{"arg1"},
			Body:              "return arg1 + 1;",
			CalledOnNullInput: false,
		},
		{
			Keyspace:          "test_keyspace",
			Name:              "test_func_no_args",
			ArgumentTypes:     []TypeInfo{},
			ArgumentNames:     []string{},
			Body:              "return 1;",
			CalledOnNullInput: false,
		},
		{
			Keyspace:          "test_keyspace",
			Name:              "test_func_null_input",
			ArgumentTypes:     []TypeInfo{intTypeInfo{}},
			ArgumentNames:     []string{"arg1"},
			Body:              "if (arg1 == null) return 0; else return arg1;",
			CalledOnNullInput: true,
		},
	}

	compileMetadata(session, keyspace, nil, nil, functions, nil, nil, nil)

	require.Len(t, keyspace.Functions, 3, "Expected to have 3 functions")
	require.Contains(t, keyspace.Functions, "test_func")
	require.Contains(t, keyspace.Functions, "test_func_no_args")
	require.Contains(t, keyspace.Functions, "test_func_null_input")

	testFunc := keyspace.Functions["test_func"]
	require.Equal(t, "test_func", testFunc.Name)
	require.Len(t, testFunc.ArgumentTypes, 1)
	require.Equal(t, TypeInt, testFunc.ArgumentTypes[0].Type())
	require.Len(t, testFunc.ArgumentNames, 1)
	require.Equal(t, "arg1", testFunc.ArgumentNames[0])
	require.Equal(t, "return arg1 + 1;", testFunc.Body)
	require.False(t, testFunc.CalledOnNullInput)

	testFuncNoArgs := keyspace.Functions["test_func_no_args"]
	require.Equal(t, "test_func_no_args", testFuncNoArgs.Name)
	require.Empty(t, testFuncNoArgs.ArgumentTypes)
	require.Empty(t, testFuncNoArgs.ArgumentNames)
	require.Equal(t, "return 1;", testFuncNoArgs.Body)
	require.False(t, testFuncNoArgs.CalledOnNullInput)

	testFuncNullInput := keyspace.Functions["test_func_null_input"]
	require.Equal(t, "test_func_null_input", testFuncNullInput.Name)
	require.Len(t, testFuncNullInput.ArgumentTypes, 1)
	require.Equal(t, TypeInt, testFuncNullInput.ArgumentTypes[0].Type())
	require.Len(t, testFuncNullInput.ArgumentNames, 1)
	require.Equal(t, "arg1", testFuncNullInput.ArgumentNames[0])
	require.Equal(t, "if (arg1 == null) return 0; else return arg1;", testFuncNullInput.Body)
	require.True(t, testFuncNullInput.CalledOnNullInput)
}

func TestCompileMetadataWithAggregates(t *testing.T) {
	session := &Session{
		cfg: ClusterConfig{
			ProtoVersion: protoVersion5,
			Logger:       NewLogger(LogLevelInfo),
		},
		types: GlobalTypes.Copy(),
	}

	keyspace := &KeyspaceMetadata{
		Name: "test_keyspace",
	}

	functions := []FunctionMetadata{
		{
			Keyspace:          "test_keyspace",
			Name:              "test_state_func",
			ArgumentTypes:     []TypeInfo{intTypeInfo{}},
			ArgumentNames:     []string{"arg1"},
			Body:              "return arg1 + 1;",
			CalledOnNullInput: false,
		},
		{
			Keyspace:          "test_keyspace",
			Name:              "test_final_func",
			ArgumentTypes:     []TypeInfo{floatTypeInfo{}},
			ArgumentNames:     []string{"arg1"},
			Body:              "return arg1 + 1;",
			CalledOnNullInput: false,
		},
	}

	aggregates := []AggregateMetadata{
		{
			Keyspace: "test_keyspace",
			Name:     "test_agg",
			ArgumentTypes: []TypeInfo{
				intTypeInfo{},
			},
			InitCond:   "0",
			StateFunc:  functions[0],
			FinalFunc:  functions[1],
			ReturnType: intTypeInfo{},
			StateType:  intTypeInfo{},
			stateFunc:  "test_state_func",
			finalFunc:  "test_final_func",
		},
		{
			Keyspace: "test_keyspace",
			Name:     "test_agg_no_final_func",
			ArgumentTypes: []TypeInfo{
				doubleTypeInfo{},
			},
			InitCond:   "0",
			StateFunc:  functions[0],
			ReturnType: doubleTypeInfo{},
			StateType:  doubleTypeInfo{},
			stateFunc:  "test_state_func",
			finalFunc:  "",
		},
	}

	compileMetadata(session, keyspace, nil, nil, functions, aggregates, nil, nil)

	require.Len(t, keyspace.Aggregates, 2, "Expected to have 2 aggregates")
	require.Contains(t, keyspace.Aggregates, "test_agg")
	require.Contains(t, keyspace.Aggregates, "test_agg_no_final_func")

	testAgg := keyspace.Aggregates["test_agg"]
	require.Equal(t, "test_agg", testAgg.Name)
	require.Len(t, testAgg.ArgumentTypes, 1)
	require.Equal(t, TypeInt, testAgg.ArgumentTypes[0].Type())
	require.Equal(t, "0", testAgg.InitCond)
	require.Equal(t, TypeInt, testAgg.ReturnType.Type())
	require.Equal(t, TypeInt, testAgg.StateType.Type())

	testAggNoFinalFunc := keyspace.Aggregates["test_agg_no_final_func"]
	require.Equal(t, "test_agg_no_final_func", testAggNoFinalFunc.Name)
	require.Len(t, testAggNoFinalFunc.ArgumentTypes, 1)
	require.Equal(t, TypeDouble, testAggNoFinalFunc.ArgumentTypes[0].Type())
	require.Equal(t, "0", testAggNoFinalFunc.InitCond)
	require.Equal(t, TypeDouble, testAggNoFinalFunc.ReturnType.Type())
	require.Equal(t, TypeDouble, testAggNoFinalFunc.StateType.Type())
}

func TestCompareTablesMetadata(t *testing.T) {
	type testCase struct {
		name           string
		table1         *TableMetadata
		table2         *TableMetadata
		expectedEquals bool
	}

	tests := []testCase{
		{
			name:           "both_nil",
			table1:         nil,
			table2:         nil,
			expectedEquals: true,
		},
		{
			name:   "table1_nil",
			table1: nil,
			table2: &TableMetadata{
				Name: "test_table",
			},
			expectedEquals: false,
		},
		{
			name: "table2_nil",
			table1: &TableMetadata{
				Name: "test_table",
			},
			table2:         nil,
			expectedEquals: false,
		},
		{
			name: "different_Name",
			table1: &TableMetadata{
				Name: "test_table_1",
			},
			table2: &TableMetadata{
				Name: "test_table_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_KeyValidator",
			table1: &TableMetadata{
				KeyValidator: "int",
			},
			table2: &TableMetadata{
				KeyValidator: "float",
			},
			expectedEquals: false,
		},
		{
			name: "different_Comparator",
			table1: &TableMetadata{
				Comparator: "int",
			},
			table2: &TableMetadata{
				Comparator: "float",
			},
			expectedEquals: false,
		},
		{
			name: "different_DefaultValidator",
			table1: &TableMetadata{
				DefaultValidator: "int",
			},
			table2: &TableMetadata{
				DefaultValidator: "float",
			},
			expectedEquals: false,
		},
		{
			name: "different_ValueAlias",
			table1: &TableMetadata{
				ValueAlias: "test_value_alias_1",
			},
			table2: &TableMetadata{
				ValueAlias: "test_value_alias_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_KeyAliases",
			table1: &TableMetadata{
				KeyAliases: []string{"test_key_alias_1"},
			},
			table2: &TableMetadata{
				KeyAliases: []string{"test_key_alias_2"},
			},
			expectedEquals: false,
		},
		{
			name: "different_ColumnAliases",
			table1: &TableMetadata{
				ColumnAliases: []string{"test_column_alias_1"},
			},
			table2: &TableMetadata{
				ColumnAliases: []string{"test_column_alias_2"},
			},
			expectedEquals: false,
		},
		{
			name: "different_OrderedColumns",
			table1: &TableMetadata{
				OrderedColumns: []string{"test_ordered_column_1"},
			},
			table2: &TableMetadata{
				OrderedColumns: []string{"test_ordered_column_2"},
			},
			expectedEquals: false,
		},
		{
			name: "different_PartitionKey",
			table1: &TableMetadata{
				PartitionKey: []*ColumnMetadata{
					{Name: "test_partition_key_1"},
				},
			},
			table2: &TableMetadata{
				PartitionKey: []*ColumnMetadata{
					{Name: "test_partition_key_2"},
				},
			},
			expectedEquals: false,
		},
		{
			name: "different_ClusteringColumns",
			table1: &TableMetadata{
				ClusteringColumns: []*ColumnMetadata{
					{Name: "col_1"},
				},
			},
			table2: &TableMetadata{
				ClusteringColumns: []*ColumnMetadata{
					{Name: "col_2"},
				},
			},
			expectedEquals: false,
		},
		{
			name: "different_Columns",
			table1: &TableMetadata{
				Columns: map[string]*ColumnMetadata{
					"col_1": {Name: "col_1"},
				},
			},
			table2: &TableMetadata{
				Columns: map[string]*ColumnMetadata{
					"col_2": {Name: "col_2"},
				},
			},
			expectedEquals: false,
		},
		{
			name: "equals",
			table1: &TableMetadata{
				Name:             "test_table",
				KeyValidator:     "int",
				Comparator:       "int",
				DefaultValidator: "int",
				ValueAlias:       "test_value_alias_1",
				KeyAliases:       []string{"test_key_alias_1"},
				ColumnAliases:    []string{"test_column_alias_1"},
				OrderedColumns:   []string{"test_ordered_column_1"},
				PartitionKey: []*ColumnMetadata{
					{Name: "test_partition_key_1"},
				},
				ClusteringColumns: []*ColumnMetadata{
					{Name: "test_clustering_column_1"},
				},
				Columns: map[string]*ColumnMetadata{
					"col_1": {Name: "col_1"},
				},
			},
			table2: &TableMetadata{
				Name:             "test_table",
				KeyValidator:     "int",
				Comparator:       "int",
				DefaultValidator: "int",
				ValueAlias:       "test_value_alias_1",
				KeyAliases:       []string{"test_key_alias_1"},
				ColumnAliases:    []string{"test_column_alias_1"},
				OrderedColumns:   []string{"test_ordered_column_1"},
				PartitionKey: []*ColumnMetadata{
					{Name: "test_partition_key_1"},
				},
				ClusteringColumns: []*ColumnMetadata{
					{Name: "test_clustering_column_1"},
				},
				Columns: map[string]*ColumnMetadata{
					"col_1": {Name: "col_1"},
				},
			},
			expectedEquals: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			equals := compareTablesMetadata(testCase.table1, testCase.table2)
			require.Equal(t, testCase.expectedEquals, equals)
		})
	}
}

func TestCompareColumnMetadata(t *testing.T) {
	type testCase struct {
		name           string
		column1        *ColumnMetadata
		column2        *ColumnMetadata
		expectedEquals bool
	}

	tests := []testCase{
		{
			name:           "both_nil",
			column1:        nil,
			column2:        nil,
			expectedEquals: true,
		},
		{
			name:    "column1_nil",
			column1: nil,
			column2: &ColumnMetadata{
				Name: "test_column",
			},
			expectedEquals: false,
		},
		{
			name: "column2_nil",
			column1: &ColumnMetadata{
				Name: "test_column",
			},
			column2:        nil,
			expectedEquals: false,
		},
		{
			name: "different_Name",
			column1: &ColumnMetadata{
				Name: "test_column_1",
			},
			column2: &ColumnMetadata{
				Name: "test_column_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_Table",
			column1: &ColumnMetadata{
				Table: "test_table_1",
			},
			column2: &ColumnMetadata{
				Table: "test_table_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_ComponentIndex",
			column1: &ColumnMetadata{
				ComponentIndex: 1,
			},
			column2: &ColumnMetadata{
				ComponentIndex: 2,
			},
		},
		{
			name: "different_Kind",
			column1: &ColumnMetadata{
				Kind: ColumnPartitionKey,
			},
			column2: &ColumnMetadata{
				Kind: ColumnClusteringKey,
			},
			expectedEquals: false,
		},
		{
			name: "different_Validator",
			column1: &ColumnMetadata{
				Validator: "test_validator_1",
			},
			column2: &ColumnMetadata{
				Validator: "test_validator_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_ClusteringOrder",
			column1: &ColumnMetadata{
				ClusteringOrder: "ASC",
			},
			column2: &ColumnMetadata{
				ClusteringOrder: "DESC",
			},
			expectedEquals: false,
		},
		{
			name: "different_Order",
			column1: &ColumnMetadata{
				Order: ASC,
			},
			column2: &ColumnMetadata{
				Order: DESC,
			},
			expectedEquals: false,
		},
		{
			name: "different_Index_Name",
			column1: &ColumnMetadata{
				Index: ColumnIndexMetadata{
					Name: "test_index_1",
				},
			},
			column2: &ColumnMetadata{
				Index: ColumnIndexMetadata{
					Name: "test_index_2",
				},
			},
			expectedEquals: false,
		},
		{
			name: "different_Index_Type",
			column1: &ColumnMetadata{
				Index: ColumnIndexMetadata{
					Type: "type_1",
				},
			},
			column2: &ColumnMetadata{
				Index: ColumnIndexMetadata{
					Type: "type_2",
				},
			},
			expectedEquals: false,
		},
		{
			name: "different_Index_Options",
			column1: &ColumnMetadata{
				Index: ColumnIndexMetadata{
					Options: map[string]interface{}{
						"test_option": "test_value",
					},
				},
			},
			column2: &ColumnMetadata{
				Index: ColumnIndexMetadata{
					Options: map[string]interface{}{
						"test_option": "test_value_2",
					},
				},
			},
			expectedEquals: false,
		},
		{
			name: "equals",
			column1: &ColumnMetadata{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				Name:            "test_column",
				ComponentIndex:  1,
				Kind:            ColumnClusteringKey,
				Validator:       "test_validator",
				Type:            intTypeInfo{},
				ClusteringOrder: "ASC",
				Order:           ASC,
				Index: ColumnIndexMetadata{
					Name: "test_index_1",
					Type: "test_type",
					Options: map[string]interface{}{
						"test_option": "test_value",
					},
				},
			},
			column2: &ColumnMetadata{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				Name:            "test_column",
				ComponentIndex:  1,
				Kind:            ColumnClusteringKey,
				Validator:       "test_validator",
				Type:            intTypeInfo{},
				ClusteringOrder: "ASC",
				Order:           ASC,
				Index: ColumnIndexMetadata{
					Name: "test_index_1",
					Type: "test_type",
					Options: map[string]interface{}{
						"test_option": "test_value",
					},
				},
			},
			expectedEquals: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			equals := compareColumnMetadata(testCase.column1, testCase.column2)
			require.Equal(t, testCase.expectedEquals, equals)
		})
	}
}

func TestCompareAggregateMetadata(t *testing.T) {
	type testCase struct {
		name           string
		aggregate1     *AggregateMetadata
		aggregate2     *AggregateMetadata
		expectedEquals bool
	}

	tests := []testCase{
		{
			name:           "both_nil",
			aggregate1:     nil,
			aggregate2:     nil,
			expectedEquals: true,
		},
		{
			name:       "aggregate1_nil",
			aggregate1: nil,
			aggregate2: &AggregateMetadata{
				Name: "test_aggregate",
			},
			expectedEquals: false,
		},
		{
			name: "aggregate2_nil",
			aggregate1: &AggregateMetadata{
				Name: "test_aggregate",
			},
			aggregate2:     nil,
			expectedEquals: false,
		},
		{
			name: "different_finalFunc",
			aggregate1: &AggregateMetadata{
				finalFunc: "test_final_func_1",
			},
			aggregate2: &AggregateMetadata{
				finalFunc: "test_final_func_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_InitCond",
			aggregate1: &AggregateMetadata{
				InitCond: "test_init_cond_1",
			},
			aggregate2: &AggregateMetadata{
				InitCond: "test_init_cond_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_returnTypeRaw",
			aggregate1: &AggregateMetadata{
				returnTypeRaw: "test_return_type_1",
			},
			aggregate2: &AggregateMetadata{
				returnTypeRaw: "test_return_type_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_stateFunc",
			aggregate1: &AggregateMetadata{
				stateFunc: "test_state_func_1",
			},
			aggregate2: &AggregateMetadata{
				stateFunc: "test_state_func_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_stateTypeRaw",
			aggregate1: &AggregateMetadata{
				stateTypeRaw: "test_state_type_1",
			},
			aggregate2: &AggregateMetadata{
				stateTypeRaw: "test_state_type_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_argumentTypesRaw",
			aggregate1: &AggregateMetadata{
				argumentTypesRaw: []string{"test_argument_type_1"},
			},
			aggregate2: &AggregateMetadata{
				argumentTypesRaw: []string{"test_argument_type_2"},
			},
			expectedEquals: false,
		},
		{
			name: "different_argumentTypesRaw_byLength",
			aggregate1: &AggregateMetadata{
				argumentTypesRaw: []string{"test_argument_type_1"},
			},
			aggregate2: &AggregateMetadata{
				argumentTypesRaw: []string{"test_argument_type_1", "test_argument_type_2"},
			},
			expectedEquals: false,
		},
		{
			name: "equals",
			aggregate1: &AggregateMetadata{
				finalFunc:        "test_final_func",
				InitCond:         "0",
				returnTypeRaw:    "test_return_type",
				stateFunc:        "test_state_func",
				stateTypeRaw:     "test_state_type",
				argumentTypesRaw: []string{"test_argument_type_1", "test_argument_type_2"},
			},
			aggregate2: &AggregateMetadata{
				finalFunc:        "test_final_func",
				InitCond:         "0",
				returnTypeRaw:    "test_return_type",
				stateFunc:        "test_state_func",
				stateTypeRaw:     "test_state_type",
				argumentTypesRaw: []string{"test_argument_type_1", "test_argument_type_2"},
			},
			expectedEquals: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			equals := compareAggregateMetadata(testCase.aggregate1, testCase.aggregate2)
			require.Equal(t, testCase.expectedEquals, equals)
		})
	}
}

func TestCompareFunctionMetadata(t *testing.T) {
	type testCase struct {
		name           string
		function1      *FunctionMetadata
		function2      *FunctionMetadata
		expectedEquals bool
	}

	tests := []testCase{
		{
			name:           "both_nil",
			function1:      nil,
			function2:      nil,
			expectedEquals: true,
		},
		{
			name:      "function1_nil",
			function1: nil,
			function2: &FunctionMetadata{
				Name: "test_function",
			},
			expectedEquals: false,
		},
		{
			name: "function2_nil",
			function1: &FunctionMetadata{
				Name: "test_function",
			},
			function2:      nil,
			expectedEquals: false,
		},
		{
			name: "different_Body",
			function1: &FunctionMetadata{
				Body: "test_body_1",
			},
			function2: &FunctionMetadata{
				Body: "test_body_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_CalledOnNullInput",
			function1: &FunctionMetadata{
				CalledOnNullInput: true,
			},
			function2: &FunctionMetadata{
				CalledOnNullInput: false,
			},
			expectedEquals: false,
		},
		{
			name: "different_Language",
			function1: &FunctionMetadata{
				Language: "test_language_1",
			},
			function2: &FunctionMetadata{
				Language: "test_language_2",
			},
			expectedEquals: false,
		},
		{
			name: "different_ArgumentNames",
			function1: &FunctionMetadata{
				ArgumentNames: []string{"test_argument_name_1"},
			},
			function2: &FunctionMetadata{
				ArgumentNames: []string{"test_argument_name_2"},
			},
			expectedEquals: false,
		},
		{
			name: "different_argumentTypesRaw",
			function1: &FunctionMetadata{
				argumentTypesRaw: []string{"test_argument_type_1"},
			},
			function2: &FunctionMetadata{
				argumentTypesRaw: []string{"test_argument_type_2"},
			},
			expectedEquals: false,
		},
		{
			name: "different_returnTypeRaw",
			function1: &FunctionMetadata{
				returnTypeRaw: "test_return_type_1",
			},
			function2: &FunctionMetadata{
				returnTypeRaw: "test_return_type_2",
			},
			expectedEquals: false,
		},
		{
			name: "equals",
			function1: &FunctionMetadata{
				Body:              "test_body",
				CalledOnNullInput: true,
				Language:          "test_language",
				ArgumentNames:     []string{"test_argument_name"},
				argumentTypesRaw:  []string{"test_argument_type"},
				returnTypeRaw:     "test_return_type",
			},
			function2: &FunctionMetadata{
				Body:              "test_body",
				CalledOnNullInput: true,
				Language:          "test_language",
				ArgumentNames:     []string{"test_argument_name"},
				argumentTypesRaw:  []string{"test_argument_type"},
				returnTypeRaw:     "test_return_type",
			},
			expectedEquals: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			equals := compareFunctionMetadata(testCase.function1, testCase.function2)
			require.Equal(t, testCase.expectedEquals, equals)
		})
	}
}

func TestKeyspaceMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var ks *KeyspaceMetadata
		clone := ks.Clone()
		require.Nil(t, clone)
	})

	t.Run("deep_copy", func(t *testing.T) {
		original := &KeyspaceMetadata{
			Name:          "test_keyspace",
			DurableWrites: true,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"replication_factor": 3,
			},
			Tables: map[string]*TableMetadata{
				"table1": {
					Name:     "table1",
					Keyspace: "test_keyspace",
				},
			},
			Functions: map[string]*FunctionMetadata{
				"func1": {
					Name:     "func1",
					Keyspace: "test_keyspace",
				},
			},
			Aggregates: map[string]*AggregateMetadata{
				"agg1": {
					Name:     "agg1",
					Keyspace: "test_keyspace",
				},
			},
			UserTypes: map[string]*UserTypeMetadata{
				"type1": {
					Name:     "type1",
					Keyspace: "test_keyspace",
				},
			},
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Name, clone.Name)
		require.Equal(t, original.DurableWrites, clone.DurableWrites)
		require.Equal(t, original.StrategyClass, clone.StrategyClass)

		// Verify maps are different instances
		require.NotSame(t, original.StrategyOptions, clone.StrategyOptions)
		require.NotSame(t, original.Tables, clone.Tables)
		require.NotSame(t, original.Functions, clone.Functions)
		require.NotSame(t, original.Aggregates, clone.Aggregates)
		require.NotSame(t, original.UserTypes, clone.UserTypes)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		clone.StrategyOptions["replication_factor"] = 5
		clone.Tables["table2"] = &TableMetadata{Name: "table2"}

		require.Equal(t, "test_keyspace", original.Name)
		require.Equal(t, 3, original.StrategyOptions["replication_factor"])
		require.Len(t, original.Tables, 1)
	})
}

func TestTableMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var table *TableMetadata
		clone := table.Clone()
		require.Nil(t, clone)
	})

	t.Run("deep_copy", func(t *testing.T) {
		original := &TableMetadata{
			Keyspace:      "test_keyspace",
			Name:          "test_table",
			KeyAliases:    []string{"key1", "key2"},
			ColumnAliases: []string{"col1", "col2"},
			PartitionKey: []*ColumnMetadata{
				{Name: "pk1", Keyspace: "test_keyspace", Table: "test_table"},
			},
			ClusteringColumns: []*ColumnMetadata{
				{Name: "cc1", Keyspace: "test_keyspace", Table: "test_table"},
			},
			Columns: map[string]*ColumnMetadata{
				"col1": {Name: "col1", Keyspace: "test_keyspace", Table: "test_table"},
			},
			OrderedColumns: []string{"col1", "col2"},
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Keyspace, clone.Keyspace)
		require.Equal(t, original.Name, clone.Name)

		// Verify slices and maps are different instances
		require.NotSame(t, original.KeyAliases, clone.KeyAliases)
		require.NotSame(t, original.ColumnAliases, clone.ColumnAliases)
		require.NotSame(t, original.PartitionKey, clone.PartitionKey)
		require.NotSame(t, original.ClusteringColumns, clone.ClusteringColumns)
		require.NotSame(t, original.Columns, clone.Columns)
		require.NotSame(t, original.OrderedColumns, clone.OrderedColumns)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		clone.KeyAliases[0] = "modified_key"
		clone.Columns["col2"] = &ColumnMetadata{Name: "col2"}

		require.Equal(t, "test_table", original.Name)
		require.Equal(t, "key1", original.KeyAliases[0])
		require.Len(t, original.Columns, 1)
	})
}

func TestColumnMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var col *ColumnMetadata
		clone := col.Clone()
		require.Nil(t, clone)
	})

	t.Run("copy", func(t *testing.T) {
		original := &ColumnMetadata{
			Keyspace:       "test_keyspace",
			Table:          "test_table",
			Name:           "test_column",
			ComponentIndex: 1,
			Kind:           ColumnPartitionKey,
			Validator:      "org.apache.cassandra.db.marshal.UTF8Type",
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Keyspace, clone.Keyspace)
		require.Equal(t, original.Table, clone.Table)
		require.Equal(t, original.Name, clone.Name)
		require.Equal(t, original.ComponentIndex, clone.ComponentIndex)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		require.Equal(t, "test_column", original.Name)
	})
}

func TestFunctionMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var fn *FunctionMetadata
		clone := fn.Clone()
		require.Nil(t, clone)
	})

	t.Run("deep_copy", func(t *testing.T) {
		original := &FunctionMetadata{
			Keyspace:          "test_keyspace",
			Name:              "test_function",
			ArgumentNames:     []string{"arg1", "arg2"},
			Body:              "return arg1 + arg2;",
			CalledOnNullInput: true,
			Language:          "java",
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Keyspace, clone.Keyspace)
		require.Equal(t, original.Name, clone.Name)
		require.Equal(t, original.Body, clone.Body)

		// Verify slices are different instances
		require.NotSame(t, original.ArgumentNames, clone.ArgumentNames)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		clone.ArgumentNames[0] = "modified_arg"

		require.Equal(t, "test_function", original.Name)
		require.Equal(t, "arg1", original.ArgumentNames[0])
	})
}

func TestAggregateMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var agg *AggregateMetadata
		clone := agg.Clone()
		require.Nil(t, clone)
	})

	t.Run("deep_copy", func(t *testing.T) {
		original := &AggregateMetadata{
			Keyspace: "test_keyspace",
			Name:     "test_aggregate",
			InitCond: "0",
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Keyspace, clone.Keyspace)
		require.Equal(t, original.Name, clone.Name)
		require.Equal(t, original.InitCond, clone.InitCond)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		require.Equal(t, "test_aggregate", original.Name)
	})
}

func TestUserTypeMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var udt *UserTypeMetadata
		clone := udt.Clone()
		require.Nil(t, clone)
	})

	t.Run("deep_copy", func(t *testing.T) {
		original := &UserTypeMetadata{
			Keyspace:   "test_keyspace",
			Name:       "test_type",
			FieldNames: []string{"field1", "field2"},
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Keyspace, clone.Keyspace)
		require.Equal(t, original.Name, clone.Name)

		// Verify slices are different instances
		require.NotSame(t, original.FieldNames, clone.FieldNames)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		clone.FieldNames[0] = "modified_field"

		require.Equal(t, "test_type", original.Name)
		require.Equal(t, "field1", original.FieldNames[0])
	})
}

func TestMaterializedViewMetadataClone(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var mv *MaterializedViewMetadata
		clone := mv.Clone()
		require.Nil(t, clone)
	})

	t.Run("deep_copy", func(t *testing.T) {
		original := &MaterializedViewMetadata{
			Keyspace: "test_keyspace",
			Name:     "test_view",
			BaseTable: &TableMetadata{
				Name:     "base_table",
				Keyspace: "test_keyspace",
			},
			Caching: map[string]string{
				"keys": "ALL",
			},
			Compaction: map[string]string{
				"class": "SizeTieredCompactionStrategy",
			},
		}

		clone := original.Clone()

		// Verify clone is not nil and has same values
		require.NotNil(t, clone)
		require.Equal(t, original.Keyspace, clone.Keyspace)
		require.Equal(t, original.Name, clone.Name)

		// Verify maps are different instances
		require.NotSame(t, original.Caching, clone.Caching)
		require.NotSame(t, original.Compaction, clone.Compaction)
		require.NotSame(t, original.BaseTable, clone.BaseTable)

		// Verify modifying clone doesn't affect original
		clone.Name = "modified"
		clone.Caching["keys"] = "NONE"

		require.Equal(t, "test_view", original.Name)
		require.Equal(t, "ALL", original.Caching["keys"])
	})
}
