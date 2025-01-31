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

	b := session.Batch(LoggedBatch)
	b = b.Query("SELECT * FROM gocql_test.batch_errors WHERE id=2 AND val=?", nil)
	if err := b.Exec(); err == nil {
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

	b := session.Batch(LoggedBatch)
	b.WithTimestamp(micros)
	b = b.Query("INSERT INTO gocql_test.batch_ts (id, val) VALUES (?, ?)", 1, "val")
	b = b.Query("INSERT INTO gocql_test.batch_ts (id, val) VALUES (?, ?)", 2, "val")

	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}

	var storedTs int64
	if err := session.Query(`SELECT writetime(val) FROM gocql_test.batch_ts WHERE id = ?`, 1).Scan(&storedTs); err != nil {
		t.Fatal(err)
	}

	if storedTs != micros {
		t.Errorf("got ts %d, expected %d", storedTs, micros)
	}
}

func TestBatch_WithNowInSeconds(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion5 {
		t.Skip("Batch now in seconds are only available on protocol >= 5")
	}

	if err := createTable(session, `CREATE TABLE IF NOT EXISTS batch_now_in_seconds (id int primary key, val text)`); err != nil {
		t.Fatal(err)
	}

	b := session.NewBatch(LoggedBatch)
	b.WithNowInSeconds(0)
	b.Query("INSERT INTO batch_now_in_seconds (id, val) VALUES (?, ?) USING TTL 20", 1, "val")
	if err := session.ExecuteBatch(b); err != nil {
		t.Fatal(err)
	}

	var remainingTTL int
	err := session.Query(`SELECT TTL(val) FROM batch_now_in_seconds WHERE id = ?`, 1).
		WithNowInSeconds(10).
		Scan(&remainingTTL)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, remainingTTL, 10)
}

func TestBatch_SetKeyspace(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion < protoVersion5 {
		t.Skip("keyspace for BATCH message is not supported in protocol < 5")
	}

	const keyspaceStmt = `
		CREATE KEYSPACE IF NOT EXISTS gocql_keyspace_override_test 
		WITH replication = {
			'class': 'SimpleStrategy', 
			'replication_factor': '1'
		};
`

	err := session.Query(keyspaceStmt).Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = createTable(session, "CREATE TABLE IF NOT EXISTS gocql_keyspace_override_test.batch_keyspace(id int, value text, PRIMARY KEY (id))")
	if err != nil {
		t.Fatal(err)
	}

	ids := []int{1, 2}
	texts := []string{"val1", "val2"}

	b := session.NewBatch(LoggedBatch).SetKeyspace("gocql_keyspace_override_test")
	b.Query("INSERT INTO batch_keyspace(id, value) VALUES (?, ?)", ids[0], texts[0])
	b.Query("INSERT INTO batch_keyspace(id, value) VALUES (?, ?)", ids[1], texts[1])
	err = session.ExecuteBatch(b)
	if err != nil {
		t.Fatal(err)
	}

	var (
		id   int
		text string
	)

	iter := session.Query("SELECT * FROM gocql_keyspace_override_test.batch_keyspace").Iter()
	defer iter.Close()

	for i := 0; iter.Scan(&id, &text); i++ {
		require.Equal(t, id, ids[i])
		require.Equal(t, text, texts[i])
	}
}
