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
	"fmt"
	"log"

	gocql "github.com/gocql/gocql"
)

// Example_nulls demonstrates how to distinguish between null and zero value when needed.
//
// Null values are unmarshalled as zero value of the type. If you need to distinguish for example between text
// column being null and empty string, you can unmarshal into *string field.
func Example_nulls() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.stringvals(id int, value text, PRIMARY KEY(id));
	insert into example.stringvals (id, value) values (1, null);
	insert into example.stringvals (id, value) values (2, '');
	insert into example.stringvals (id, value) values (3, 'hello');
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	scanner := session.Query(`SELECT id, value FROM stringvals`).Iter().Scanner()
	for scanner.Next() {
		var (
			id  int32
			val *string
		)
		err := scanner.Scan(&id, &val)
		if err != nil {
			log.Fatal(err)
		}
		if val != nil {
			fmt.Printf("Row %d is %q\n", id, *val)
		} else {
			fmt.Printf("Row %d is null\n", id)
		}

	}
	err = scanner.Err()
	if err != nil {
		log.Fatal(err)
	}
	// Row 1 is null
	// Row 2 is ""
	// Row 3 is "hello"
}
