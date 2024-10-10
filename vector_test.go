//go:build all || cassandra
// +build all cassandra

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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVector_Marshaler(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_fixed(id int primary key, vec vector<float, 3>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_variable(id int primary key, vec vector<text, 4>);`)
	if err != nil {
		t.Fatal(err)
	}

	insertFixVec := []float32{8, 2.5, -5.0}
	err = session.Query("INSERT INTO vector_fixed(id, vec) VALUES(?, ?)", 1, insertFixVec).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var vf []float32
	err = session.Query("SELECT vec FROM vector_fixed WHERE id = ?", 1).Scan(&vf)
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "fixed size element vector", insertFixVec, vf)

	longText := randomText(500)
	insertVarVec := []string{"apache", "cassandra", longText, "gocql"}
	err = session.Query("INSERT INTO vector_variable(id, vec) VALUES(?, ?)", 1, insertVarVec).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var vv []string
	err = session.Query("SELECT vec FROM vector_variable WHERE id = ?", 1).Scan(&vv)
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "variable size element vector", insertVarVec, vv)
}

func TestVector_Empty(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_fixed_null(id int primary key, vec vector<float, 3>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_variable_null(id int primary key, vec vector<text, 4>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = session.Query("INSERT INTO vector_fixed_null(id) VALUES(?)", 1).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var vf []float32
	err = session.Query("SELECT vec FROM vector_fixed_null WHERE id = ?", 1).Scan(&vf)
	if err != nil {
		t.Fatal(err)
	}
	assertTrue(t, "fixed size element vector is empty", vf == nil)

	err = session.Query("INSERT INTO vector_variable_null(id) VALUES(?)", 1).Exec()
	if err != nil {
		t.Fatal(err)
	}
	var vv []string
	err = session.Query("SELECT vec FROM vector_variable_null WHERE id = ?", 1).Scan(&vv)
	if err != nil {
		t.Fatal(err)
	}
	assertTrue(t, "variable size element vector is empty", vv == nil)
}

func TestVector_MissingDimension(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}

	err := createTable(session, `CREATE TABLE IF NOT EXISTS gocql_test.vector_fixed(id int primary key, vec vector<float, 3>);`)
	if err != nil {
		t.Fatal(err)
	}

	err = session.Query("INSERT INTO vector_fixed(id, vec) VALUES(?, ?)", 1, []float32{8, -5.0}).Exec()
	require.Error(t, err, "expected vector with 3 dimensions, received 2")
}
