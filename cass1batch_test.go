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
	"strings"
	"testing"
)

func TestProto1BatchInsert(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.large (id int primary key)"); err != nil {
		t.Fatal(err)
	}

	begin := "BEGIN BATCH"
	end := "APPLY BATCH"
	query := "INSERT INTO large (id) VALUES (?)"
	fullQuery := strings.Join([]string{begin, query, end}, "\n")
	args := []interface{}{5}
	if err := session.Query(fullQuery, args...).Consistency(Quorum).Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestShouldPrepareFunction(t *testing.T) {
	var shouldPrepareTests = []struct {
		Stmt   string
		Result bool
	}{
		{`
      BEGIN BATCH
        INSERT INTO users (userID, password)
        VALUES ('smith', 'secret')
      APPLY BATCH
    ;
      `, true},
		{`INSERT INTO users (userID, password, name) VALUES ('user2', 'ch@ngem3b', 'second user')`, true},
		{`BEGIN COUNTER BATCH UPDATE stats SET views = views + 1 WHERE pageid = 1 APPLY BATCH`, true},
		{`delete name from users where userID = 'smith';`, true},
		{`  UPDATE users SET password = 'secret' WHERE userID = 'smith'   `, true},
		{`CREATE TABLE users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint
      );`, false},
	}

	for _, test := range shouldPrepareTests {
		q := &Query{stmt: test.Stmt, routingInfo: &queryRoutingInfo{}}
		if got := q.shouldPrepare(); got != test.Result {
			t.Fatalf("%q: got %v, expected %v\n", test.Stmt, got, test.Result)
		}
	}
}
