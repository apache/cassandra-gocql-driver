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
	"time"
)

func TestBatch_Errors(t *testing.T) {
	if *flagProto == 1 {
	}

	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion2 {
		t.Skip("atomic batches not supported. Please use Cassandra >= 2.0")
	}

	if err := createTable(session, `CREATE TABLE gocql_test.batch_errors (id int primary key, val inet)`); err != nil {
		t.Fatal(err)
	}

	b := session.NewBatch(LoggedBatch)
	b.Query("SELECT * FROM batch_errors WHERE id=2 AND val=?", nil)
	if err := session.ExecuteBatch(b); err == nil {
		t.Fatal("expected to get error for invalid query in batch")
	}
}

func TestBatch_WithTimestamp(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("Batch timestamps are only available on protocol >= 3")
	}

	if err := createTable(session, `CREATE TABLE gocql_test.batch_ts (id int primary key, val text)`); err != nil {
		t.Fatal(err)
	}

	micros := time.Now().UnixNano()/1e3 - 1000

	b := session.NewBatch(LoggedBatch)
	b.WithTimestamp(micros)
	b.Query("INSERT INTO batch_ts (id, val) VALUES (?, ?)", 1, "val")
	if err := session.ExecuteBatch(b); err != nil {
		t.Fatal(err)
	}

	var storedTs int64
	if err := session.Query(`SELECT writetime(val) FROM batch_ts WHERE id = ?`, 1).Scan(&storedTs); err != nil {
		t.Fatal(err)
	}

	if storedTs != micros {
		t.Errorf("got ts %d, expected %d", storedTs, micros)
	}
}
