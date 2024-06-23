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

package gocql_test

import (
	"context"
	"fmt"
	"log"

	gocql "github.com/gocql/gocql"
)

// Example_userDefinedTypesMap demonstrates how to work with user-defined types as maps.
// See also Example_userDefinedTypesStruct and examples for UDTMarshaler and UDTUnmarshaler if you want to map to structs.
func Example_userDefinedTypesMap() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create type example.my_udt (field_a text, field_b int);
	create table example.my_udt_table(pk int, value frozen<my_udt>, PRIMARY KEY(pk));
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	value := map[string]interface{}{
		"field_a": "a value",
		"field_b": 42,
	}
	err = session.Query("INSERT INTO example.my_udt_table (pk, value) VALUES (?, ?)",
		1, value).WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}

	var readValue map[string]interface{}

	err = session.Query("SELECT value FROM example.my_udt_table WHERE pk = 1").WithContext(ctx).Scan(&readValue)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(readValue["field_a"])
	fmt.Println(readValue["field_b"])
	// a value
	// 42
}
