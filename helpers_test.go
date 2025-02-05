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

func TestGetCassandraTypeInfo_Set(t *testing.T) {
	typ := getCassandraTypeInfo(protoVersion4, "set<text>")
	set, ok := typ.(CollectionType)
	if !ok {
		t.Fatalf("expected CollectionType got %T", typ)
	} else if set.typ != TypeSet {
		t.Fatalf("expected type %v got %v", TypeSet, set.typ)
	}

	inner, ok := set.Elem.(TypeInfo)
	if !ok {
		t.Fatalf("expected to get TypeInfo got %T", set.Elem)
	} else if inner.Type() != TypeText {
		t.Fatalf("expected to get %v got %v for set value", TypeText, set.typ)
	}
}

func TestGetCassandraTypeInfo(t *testing.T) {
	tests := []struct {
		input string
		exp   TypeInfo
	}{
		{
			"set<text>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeSet},
				Elem:       TypeText,
			},
		},
		{
			"map<text, varchar>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeMap},

				Key:  TypeText,
				Elem: TypeVarchar,
			},
		},
		{
			"list<int>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
				Elem:       TypeInt,
			},
		},
		{
			"tuple<int, int, text>", TupleTypeInfo{
				Elems: []TypeInfo{
					TypeInt,
					TypeInt,
					TypeText,
				},
			},
		},
		{
			"frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeMap},

				Key: TypeText,
				Elem: CollectionType{
					NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
					Elem: TupleTypeInfo{
						Elems: []TypeInfo{
							TypeInt,
							TypeInt,
						},
					},
				},
			},
		},
		{
			"frozen<tuple<frozen<tuple<text, frozen<list<frozen<tuple<int, int>>>>>>, frozen<tuple<text, frozen<list<frozen<tuple<int, int>>>>>>,  frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>>>",
			TupleTypeInfo{
				Elems: []TypeInfo{
					TupleTypeInfo{
						Elems: []TypeInfo{
							TypeText,
							CollectionType{
								NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
								Elem: TupleTypeInfo{
									Elems: []TypeInfo{
										TypeInt,
										TypeInt,
									},
								},
							},
						},
					},
					TupleTypeInfo{
						Elems: []TypeInfo{
							TypeText,
							CollectionType{
								NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
								Elem: TupleTypeInfo{
									Elems: []TypeInfo{
										TypeInt,
										TypeInt,
									},
								},
							},
						},
					},
					CollectionType{
						NativeType: NativeType{proto: byte(protoVersion4), typ: TypeMap},
						Key:        TypeText,
						Elem: CollectionType{
							NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
							Elem: TupleTypeInfo{
								Elems: []TypeInfo{
									TypeInt,
									TypeInt,
								},
							},
						},
					},
				},
			},
		},
		{
			"frozen<tuple<frozen<tuple<int, int>>, int, frozen<tuple<int, int>>>>", TupleTypeInfo{
				Elems: []TypeInfo{
					TupleTypeInfo{
						Elems: []TypeInfo{
							TypeInt,
							TypeInt,
						},
					},
					TypeInt,
					TupleTypeInfo{
						Elems: []TypeInfo{
							TypeInt,
							TypeInt,
						},
					},
				},
			},
		},
		{
			"frozen<map<frozen<tuple<int, int>>, int>>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeMap},
				Key: TupleTypeInfo{
					Elems: []TypeInfo{
						TypeInt,
						TypeInt,
					},
				},
				Elem: TypeInt,
			},
		},
		{
			"set<smallint>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeSet},
				Elem:       TypeSmallInt,
			},
		},
		{
			"list<tinyint>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
				Elem:       TypeTinyInt,
			},
		},
		{"smallint", TypeSmallInt},
		{"tinyint", TypeTinyInt},
		{"duration", TypeDuration},
		{"date", TypeDate},
		{
			"list<date>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeList},
				Elem:       TypeDate,
			},
		},
		{
			"set<duration>", CollectionType{
				NativeType: NativeType{proto: byte(protoVersion4), typ: TypeSet},
				Elem:       TypeDuration,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got := getCassandraTypeInfo(protoVersion4, test.input)

			// TODO(zariel): define an equal method on the types?
			if !reflect.DeepEqual(got, test.exp) {
				t.Fatalf("expected %v got %v", test.exp, got)
			}
		})
	}
}
