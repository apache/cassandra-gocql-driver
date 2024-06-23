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

func TestGetCassandraType_Set(t *testing.T) {
	typ := getCassandraType("set<text>", &defaultLogger{})
	set, ok := typ.(CollectionType)
	if !ok {
		t.Fatalf("expected CollectionType got %T", typ)
	} else if set.typ != TypeSet {
		t.Fatalf("expected type %v got %v", TypeSet, set.typ)
	}

	inner, ok := set.Elem.(NativeType)
	if !ok {
		t.Fatalf("expected to get NativeType got %T", set.Elem)
	} else if inner.typ != TypeText {
		t.Fatalf("expected to get %v got %v for set value", TypeText, set.typ)
	}
}

func TestGetCassandraType(t *testing.T) {
	tests := []struct {
		input string
		exp   TypeInfo
	}{
		{
			"set<text>", CollectionType{
				NativeType: NativeType{typ: TypeSet},

				Elem: NativeType{typ: TypeText},
			},
		},
		{
			"map<text, varchar>", CollectionType{
				NativeType: NativeType{typ: TypeMap},

				Key:  NativeType{typ: TypeText},
				Elem: NativeType{typ: TypeVarchar},
			},
		},
		{
			"list<int>", CollectionType{
				NativeType: NativeType{typ: TypeList},
				Elem:       NativeType{typ: TypeInt},
			},
		},
		{
			"tuple<int, int, text>", TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},

				Elems: []TypeInfo{
					NativeType{typ: TypeInt},
					NativeType{typ: TypeInt},
					NativeType{typ: TypeText},
				},
			},
		},
		{
			"frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>", CollectionType{
				NativeType: NativeType{typ: TypeMap},

				Key: NativeType{typ: TypeText},
				Elem: CollectionType{
					NativeType: NativeType{typ: TypeList},
					Elem: TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},

						Elems: []TypeInfo{
							NativeType{typ: TypeInt},
							NativeType{typ: TypeInt},
						},
					},
				},
			},
		},
		{
			"frozen<tuple<frozen<tuple<text, frozen<list<frozen<tuple<int, int>>>>>>, frozen<tuple<text, frozen<list<frozen<tuple<int, int>>>>>>,  frozen<map<text, frozen<list<frozen<tuple<int, int>>>>>>>>",
			TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},
				Elems: []TypeInfo{
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},
						Elems: []TypeInfo{
							NativeType{typ: TypeText},
							CollectionType{
								NativeType: NativeType{typ: TypeList},
								Elem: TupleTypeInfo{
									NativeType: NativeType{typ: TypeTuple},
									Elems: []TypeInfo{
										NativeType{typ: TypeInt},
										NativeType{typ: TypeInt},
									},
								},
							},
						},
					},
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},
						Elems: []TypeInfo{
							NativeType{typ: TypeText},
							CollectionType{
								NativeType: NativeType{typ: TypeList},
								Elem: TupleTypeInfo{
									NativeType: NativeType{typ: TypeTuple},
									Elems: []TypeInfo{
										NativeType{typ: TypeInt},
										NativeType{typ: TypeInt},
									},
								},
							},
						},
					},
					CollectionType{
						NativeType: NativeType{typ: TypeMap},
						Key:        NativeType{typ: TypeText},
						Elem: CollectionType{
							NativeType: NativeType{typ: TypeList},
							Elem: TupleTypeInfo{
								NativeType: NativeType{typ: TypeTuple},
								Elems: []TypeInfo{
									NativeType{typ: TypeInt},
									NativeType{typ: TypeInt},
								},
							},
						},
					},
				},
			},
		},
		{
			"frozen<tuple<frozen<tuple<int, int>>, int, frozen<tuple<int, int>>>>", TupleTypeInfo{
				NativeType: NativeType{typ: TypeTuple},

				Elems: []TypeInfo{
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},

						Elems: []TypeInfo{
							NativeType{typ: TypeInt},
							NativeType{typ: TypeInt},
						},
					},
					NativeType{typ: TypeInt},
					TupleTypeInfo{
						NativeType: NativeType{typ: TypeTuple},

						Elems: []TypeInfo{
							NativeType{typ: TypeInt},
							NativeType{typ: TypeInt},
						},
					},
				},
			},
		},
		{
			"frozen<map<frozen<tuple<int, int>>, int>>", CollectionType{
				NativeType: NativeType{typ: TypeMap},

				Key: TupleTypeInfo{
					NativeType: NativeType{typ: TypeTuple},

					Elems: []TypeInfo{
						NativeType{typ: TypeInt},
						NativeType{typ: TypeInt},
					},
				},
				Elem: NativeType{typ: TypeInt},
			},
		},
		{
			"set<smallint>", CollectionType{
				NativeType: NativeType{typ: TypeSet},
				Elem:       NativeType{typ: TypeSmallInt},
			},
		},
		{
			"list<tinyint>", CollectionType{
				NativeType: NativeType{typ: TypeList},
				Elem:       NativeType{typ: TypeTinyInt},
			},
		},
		{"smallint", NativeType{typ: TypeSmallInt}},
		{"tinyint", NativeType{typ: TypeTinyInt}},
		{"duration", NativeType{typ: TypeDuration}},
		{"date", NativeType{typ: TypeDate}},
		{
			"list<date>", CollectionType{
				NativeType: NativeType{typ: TypeList},
				Elem:       NativeType{typ: TypeDate},
			},
		},
		{
			"set<duration>", CollectionType{
				NativeType: NativeType{typ: TypeSet},
				Elem:       NativeType{typ: TypeDuration},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got := getCassandraType(test.input, &defaultLogger{})

			// TODO(zariel): define an equal method on the types?
			if !reflect.DeepEqual(got, test.exp) {
				t.Fatalf("expected %v got %v", test.exp, got)
			}
		})
	}
}
