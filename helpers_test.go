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

func TestGetCassandraTypeInfo_Set(t *testing.T) {
	typ, err := GlobalTypes.typeInfoFromString(protoVersion4, "set<text>")
	if err != nil {
		t.Fatal(err)
	}
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
				typ:  TypeSet,
				Elem: varcharLikeTypeInfo{typ: TypeText},
			},
		},
		{
			"map<text, varchar>", CollectionType{
				typ:  TypeMap,
				Key:  varcharLikeTypeInfo{typ: TypeText},
				Elem: varcharLikeTypeInfo{typ: TypeVarchar},
			},
		},
		{
			"list<int>", CollectionType{
				typ:  TypeList,
				Elem: intTypeInfo{},
			},
		},
		{
			"tuple<int, int, text>", TupleTypeInfo{
				Elems: []TypeInfo{
					intTypeInfo{},
					intTypeInfo{},
					varcharLikeTypeInfo{typ: TypeText},
				},
			},
		},
		{
			"frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>", CollectionType{
				typ: TypeMap,
				Key: varcharLikeTypeInfo{typ: TypeText},
				Elem: CollectionType{
					typ: TypeList,
					Elem: TupleTypeInfo{
						Elems: []TypeInfo{
							intTypeInfo{},
							intTypeInfo{},
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
							varcharLikeTypeInfo{typ: TypeText},
							CollectionType{
								typ: TypeList,
								Elem: TupleTypeInfo{
									Elems: []TypeInfo{
										intTypeInfo{},
										intTypeInfo{},
									},
								},
							},
						},
					},
					TupleTypeInfo{
						Elems: []TypeInfo{
							varcharLikeTypeInfo{typ: TypeText},
							CollectionType{
								typ: TypeList,
								Elem: TupleTypeInfo{
									Elems: []TypeInfo{
										intTypeInfo{},
										intTypeInfo{},
									},
								},
							},
						},
					},
					CollectionType{
						typ: TypeMap,
						Key: varcharLikeTypeInfo{typ: TypeText},
						Elem: CollectionType{
							typ: TypeList,
							Elem: TupleTypeInfo{
								Elems: []TypeInfo{
									intTypeInfo{},
									intTypeInfo{},
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
							intTypeInfo{},
							intTypeInfo{},
						},
					},
					intTypeInfo{},
					TupleTypeInfo{
						Elems: []TypeInfo{
							intTypeInfo{},
							intTypeInfo{},
						},
					},
				},
			},
		},
		{
			"frozen<map<frozen<tuple<int, int>>, int>>", CollectionType{
				typ: TypeMap,
				Key: TupleTypeInfo{
					Elems: []TypeInfo{
						intTypeInfo{},
						intTypeInfo{},
					},
				},
				Elem: intTypeInfo{},
			},
		},
		{
			"set<smallint>", CollectionType{
				typ:  TypeSet,
				Elem: smallIntTypeInfo{},
			},
		},
		{
			"list<tinyint>", CollectionType{
				typ:  TypeList,
				Elem: tinyIntTypeInfo{},
			},
		},
		{"smallint", smallIntTypeInfo{}},
		{"tinyint", tinyIntTypeInfo{}},
		{"duration", durationTypeInfo{}},
		{"date", dateTypeInfo{}},
		{
			"list<date>", CollectionType{
				typ:  TypeList,
				Elem: dateTypeInfo{},
			},
		},
		{
			"set<duration>", CollectionType{
				typ:  TypeSet,
				Elem: durationTypeInfo{},
			},
		},
		{
			"vector<float, 3>", VectorType{
				SubType:    floatTypeInfo{},
				Dimensions: 3,
			},
		},
		{
			"vector<vector<float, 3>, 5>", VectorType{
				SubType: VectorType{
					SubType:    floatTypeInfo{},
					Dimensions: 3,
				},
				Dimensions: 5,
			},
		},
		{
			"vector<map<uuid,timestamp>, 5>", VectorType{
				SubType: CollectionType{
					typ:  TypeMap,
					Key:  uuidType{},
					Elem: timestampTypeInfo{},
				},
				Dimensions: 5,
			},
		},
		{
			"vector<frozen<tuple<int, float>>, 100>", VectorType{
				SubType: TupleTypeInfo{
					Elems: []TypeInfo{
						intTypeInfo{},
						floatTypeInfo{},
					},
				},
				Dimensions: 100,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := GlobalTypes.typeInfoFromString(protoVersion4, test.input)
			if err != nil {
				t.Fatal(err)
			}

			// TODO(zariel): define an equal method on the types?
			if !reflect.DeepEqual(got, test.exp) {
				t.Fatalf("expected %v got %v", test.exp, got)
			}
		})
	}
}
