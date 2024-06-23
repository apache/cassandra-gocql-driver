//go:build all || integration
// +build all integration

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
	"context"
	"fmt"
	"testing"
)

// Keyspace_table checks if Query.Keyspace() is updated based on prepared statement
func TestKeyspaceTable(t *testing.T) {
	cluster := createCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("createSession:", err)
	}

	cluster.Keyspace = "wrong_keyspace"

	keyspace := "test1"
	table := "table1"

	err = createTable(session, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		t.Fatal("unable to drop keyspace:", err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : 1
	}`, keyspace))

	if err != nil {
		t.Fatal("unable to create keyspace:", err)
	}

	if err := session.control.awaitSchemaAgreement(); err != nil {
		t.Fatal(err)
	}

	err = createTable(session, fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, keyspace, table))

	if err != nil {
		t.Fatal("unable to create table:", err)
	}

	if err := session.control.awaitSchemaAgreement(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// insert a row
	if err := session.Query(`INSERT INTO test1.table1(pk, ck, v) VALUES (?, ?, ?)`,
		1, 2, 3).WithContext(ctx).Consistency(One).Exec(); err != nil {
		t.Fatal(err)
	}

	var pk int

	/* Search for a specific set of records whose 'pk' column matches
	 * the value of inserted row. */
	qry := session.Query(`SELECT pk FROM test1.table1 WHERE pk = ? LIMIT 1`,
		1).WithContext(ctx).Consistency(One)
	if err := qry.Scan(&pk); err != nil {
		t.Fatal(err)
	}

	// cluster.Keyspace was set to "wrong_keyspace", but during prepering statement
	// Keyspace in Query should be changed to "test" and Table should be changed to table1
	assertEqual(t, "qry.Keyspace()", "test1", qry.Keyspace())
	assertEqual(t, "qry.Table()", "table1", qry.Table())
}
