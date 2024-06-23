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
	"testing"
)

func TestErrorsParse(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, `CREATE TABLE gocql_test.errors_parse (id int primary key)`); err != nil {
		t.Fatal("create:", err)
	}

	if err := createTable(session, `CREATE TABLE gocql_test.errors_parse (id int primary key)`); err == nil {
		t.Fatal("Should have gotten already exists error from cassandra server.")
	} else {
		switch e := err.(type) {
		case *RequestErrAlreadyExists:
			if e.Table != "errors_parse" {
				t.Fatalf("expected error table to be 'errors_parse' but was %q", e.Table)
			}
		default:
			t.Fatalf("expected to get RequestErrAlreadyExists instead got %T", e)
		}
	}
}
