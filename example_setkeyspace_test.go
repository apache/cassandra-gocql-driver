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

package gocql_test

import (
	"context"
	"fmt"
	"log"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Example_setKeyspace demonstrates the SetKeyspace method that allows
// specifying keyspace per query, available with Protocol 5+ (Cassandra 4.0+).
//
// This example shows the complete keyspace precedence hierarchy:
// 1. Keyspace in CQL query string (keyspace.table) - HIGHEST precedence
// 2. SetKeyspace() method - MIDDLE precedence
// 3. Default session keyspace - LOWEST precedence
func Example_setKeyspace() {
	/* The example assumes the following CQL was used to setup the keyspaces:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create keyspace example2 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create keyspace example3 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.users(id int, name text, PRIMARY KEY(id));
	create table example2.users(id int, name text, PRIMARY KEY(id));
	create table example3.users(id int, name text, PRIMARY KEY(id));
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.ProtoVersion = 5     // SetKeyspace requires Protocol 5+, available in Cassandra 4.0+
	cluster.Keyspace = "example" // Set a default keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	// Example 1: Keyspace Precedence Hierarchy Demonstration
	fmt.Println("Demonstrating complete keyspace precedence hierarchy:")
	fmt.Println("1. Keyspace in CQL (keyspace.table) - HIGHEST")
	fmt.Println("2. SetKeyspace() method - MIDDLE")
	fmt.Println("3. Default session keyspace - LOWEST")
	fmt.Println()

	// Insert test data
	// Default keyspace (example) - lowest precedence
	err = session.Query("INSERT INTO users (id, name) VALUES (?, ?)").
		Bind(1, "Alice").
		ExecContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// SetKeyspace overrides default - middle precedence
	err = session.Query("INSERT INTO users (id, name) VALUES (?, ?)").
		SetKeyspace("example2").
		Bind(1, "Bob").
		ExecContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Fully qualified table name - highest precedence
	err = session.Query("INSERT INTO example3.users (id, name) VALUES (?, ?)").
		Bind(1, "Charlie").
		ExecContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Example 2: Fully qualified table names override SetKeyspace
	fmt.Println("Example 2: Fully qualified table names take precedence over SetKeyspace:")

	// This query sets keyspace to "example2" via SetKeyspace, but the fully qualified
	// table name "example3.users" takes precedence - query will target example3
	err = session.Query("INSERT INTO example3.users (id, name) VALUES (?, ?)").
		SetKeyspace("example2"). // This is IGNORED because CQL has "example3.users"
		Bind(2, "Diana").
		ExecContext(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Inserted Diana into example3.users despite SetKeyspace(\"example2\")")

	// Verify data went to example3, not example2
	var count int
	iter := session.Query("SELECT COUNT(*) FROM users").
		SetKeyspace("example2").
		IterContext(ctx)
	if iter.Scan(&count) {
		fmt.Printf("Count in example2: %d (only Bob)\n", count)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	iter = session.Query("SELECT COUNT(*) FROM users").
		SetKeyspace("example3").
		IterContext(ctx)
	if iter.Scan(&count) {
		fmt.Printf("Count in example3: %d (Charlie and Diana)\n", count)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	// Example 3: SetKeyspace overrides default keyspace
	fmt.Println("\nExample 3: SetKeyspace overrides default keyspace:")

	// Query using default keyspace (no SetKeyspace)
	var id int
	var name string
	iter = session.Query("SELECT id, name FROM users WHERE id = ?", 1).
		IterContext(ctx) // Uses default keyspace "example"
	if iter.Scan(&id, &name) {
		fmt.Printf("Default keyspace (example): ID %d, Name %s\n", id, name)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	// SetKeyspace overrides default
	iter = session.Query("SELECT id, name FROM users WHERE id = ?", 1).
		SetKeyspace("example2"). // Override default keyspace
		IterContext(ctx)
	if iter.Scan(&id, &name) {
		fmt.Printf("SetKeyspace override (example2): ID %d, Name %s\n", id, name)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	// Example 4: Mixed query patterns in one workflow
	fmt.Println("\nExample 4: Using all precedence levels in one workflow:")

	// Query from default keyspace
	iter = session.Query("SELECT name FROM users WHERE id = 1").IterContext(ctx)
	if iter.Scan(&name) {
		fmt.Printf("Default (example): %s\n", name)
	}
	iter.Close()

	// Query using SetKeyspace
	iter = session.Query("SELECT name FROM users WHERE id = 1").
		SetKeyspace("example2").IterContext(ctx)
	if iter.Scan(&name) {
		fmt.Printf("SetKeyspace (example2): %s\n", name)
	}
	iter.Close()

	// Query using fully qualified table name (ignores both default and SetKeyspace)
	iter = session.Query("SELECT name FROM example3.users WHERE id = 1").
		SetKeyspace("example2"). // This is ignored due to qualified table name
		IterContext(ctx)
	if iter.Scan(&name) {
		fmt.Printf("Qualified name (example3): %s\n", name)
	}
	iter.Close()

	// Demonstrating complete keyspace precedence hierarchy:
	// 1. Keyspace in CQL (keyspace.table) - HIGHEST
	// 2. SetKeyspace() method - MIDDLE
	// 3. Default session keyspace - LOWEST
	//
	// Example 2: Fully qualified table names take precedence over SetKeyspace:
	// Inserted Diana into example3.users despite SetKeyspace("example2")
	// Count in example2: 1 (only Bob)
	// Count in example3: 2 (Charlie and Diana)
	//
	// Example 3: SetKeyspace overrides default keyspace:
	// Default keyspace (example): ID 1, Name Alice
	// SetKeyspace override (example2): ID 1, Name Bob
	//
	// Example 4: Using all precedence levels in one workflow:
	// Default (example): Alice
	// SetKeyspace (example2): Bob
	// Qualified name (example3): Charlie
}
