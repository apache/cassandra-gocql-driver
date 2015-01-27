// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"strconv"
	"testing"
)

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

	// map
	assertParseNonCompositeType(
		t,
		"org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.BytesType)",
		assertTypeInfo{
			Type: TypeMap,
			Key:  &assertTypeInfo{Type: TypeUUID},
			Elem: &assertTypeInfo{Type: TypeBlob},
		},
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
			assertTypeInfo{Type: TypeVarchar},
		},
		nil,
	)
	assertParseCompositeType(
		t,
		"org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.DateType,org.apache.cassandra.db.marshal.UTF8Type)",
		[]assertTypeInfo{
			assertTypeInfo{Type: TypeTimestamp},
			assertTypeInfo{Type: TypeVarchar},
		},
		nil,
	)
	assertParseCompositeType(
		t,
		"org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(726f77735f6d6572676564:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.LongType)))",
		[]assertTypeInfo{
			assertTypeInfo{Type: TypeVarchar},
		},
		map[string]assertTypeInfo{
			"rows_merged": assertTypeInfo{
				Type: TypeMap,
				Key:  &assertTypeInfo{Type: TypeInt},
				Elem: &assertTypeInfo{Type: TypeBigInt},
			},
		},
	)
}

//---------------------------------------
// some code to assert the parser result
//---------------------------------------

type assertTypeInfo struct {
	Type     Type
	Reversed bool
	Elem     *assertTypeInfo
	Key      *assertTypeInfo
	Custom   string
}

func assertParseNonCompositeType(
	t *testing.T,
	def string,
	typeExpected assertTypeInfo,
) {

	result := parseType(def)
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

func assertParseCompositeType(
	t *testing.T,
	def string,
	typesExpected []assertTypeInfo,
	collectionsExpected map[string]assertTypeInfo,
) {

	result := parseType(def)
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
	if result.collections == nil {
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
		if typeActual.Type != typeExpected.Type {
			t.Errorf("%s: Expected to parse Type to %s but was %s", context, typeExpected.Type, typeActual.Type)
		}
		// check the custom
		if typeActual.Custom != typeExpected.Custom {
			t.Errorf("%s: Expected to parse Custom %s but was %s", context, typeExpected.Custom, typeActual.Custom)
		}
		// check the elem
		if typeActual.Elem == nil && typeExpected.Elem != nil {
			t.Errorf("%s: Expected to parse Elem, but was nil ", context)
		} else if typeExpected.Elem == nil && typeActual.Elem != nil {
			t.Errorf("%s: Expected to not parse Elem, but was %+v", context, typeActual.Elem)
		} else if typeActual.Elem != nil && typeExpected.Elem != nil {
			assertParseNonCompositeTypes(
				t,
				context+".Elem",
				[]assertTypeInfo{*typeExpected.Elem},
				[]TypeInfo{*typeActual.Elem},
			)
		}
		// check the key
		if typeActual.Key == nil && typeExpected.Key != nil {
			t.Errorf("%s: Expected to parse Key, but was nil ", context)
		} else if typeExpected.Key == nil && typeActual.Key != nil {
			t.Errorf("%s: Expected to not parse Key, but was %+v", context, typeActual.Key)
		} else if typeActual.Key != nil && typeExpected.Key != nil {
			assertParseNonCompositeTypes(
				t,
				context+".Key",
				[]assertTypeInfo{*typeExpected.Key},
				[]TypeInfo{*typeActual.Key},
			)
		}
	}
}
