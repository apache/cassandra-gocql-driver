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

	"github.com/gocql/gocql"
)

// Example_batch demonstrates how to execute a batch of statements.
func Example_batch() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.batches(pk int, ck int, description text, PRIMARY KEY(pk, ck));
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

	b := session.Batch(gocql.UnloggedBatch).WithContext(ctx)
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       "INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		Args:       []interface{}{1, 2, "1.2"},
		Idempotent: true,
	})
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       "INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		Args:       []interface{}{1, 3, "1.3"},
		Idempotent: true,
	})

	err = session.ExecuteBatch(b)
	if err != nil {
		log.Fatal(err)
	}

	err = b.Query("INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)", 1, 4, "1.4").
		Query("INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)", 1, 5, "1.5").
		Exec()
	if err != nil {
		log.Fatal(err)
	}

	scanner := session.Query("SELECT pk, ck, description FROM example.batches").Iter().Scanner()
	for scanner.Next() {
		var pk, ck int32
		var description string
		err = scanner.Scan(&pk, &ck, &description)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(pk, ck, description)
	}
	// 1 2 1.2
	// 1 3 1.3
	// 1 4 1.4
	// 1 5 1.5
}
