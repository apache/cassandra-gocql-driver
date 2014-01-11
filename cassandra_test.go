// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bytes"
	"flag"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto   = flag.Int("proto", 2, "protcol version")
	flagCQL     = flag.String("cql", "3.0.0", "CQL version")
)

var initOnce sync.Once

func createSession(t *testing.T) *Session {
	cluster := NewCluster(strings.Split(*flagCluster, ",")...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("createSession:", err)
	}

	initOnce.Do(func() {
		// Drop and re-create the keyspace once. Different tests should use their own
		// individual tables, but can assume that the table does not exist before.
		if err := session.Query(`DROP KEYSPACE gocql_test`).Exec(); err != nil {
			t.Log("drop keyspace:", err)
		}
		if err := session.Query(`CREATE KEYSPACE gocql_test
			WITH replication = {
				'class' : 'SimpleStrategy',
				'replication_factor' : 1
			}`).Exec(); err != nil {
			t.Fatal("create keyspace:", err)
		}
	})

	if err := session.Query(`USE gocql_test`).Exec(); err != nil {
		t.Fatal("createSession:", err)
	}

	return session
}

func TestEmptyHosts(t *testing.T) {
	cluster := NewCluster()
	if session, err := cluster.CreateSession(); err == nil {
		session.Close()
		t.Error("expected err, got nil")
	}
}

func TestCRUD(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE page (
			title       varchar,
			revid       timeuuid,
			body        varchar,
			views       bigint,
			protected   boolean,
			modified    timestamp,
			tags        set<varchar>,
			attachments map<varchar, text>,
			PRIMARY KEY (title, revid)
		)`).Exec(); err != nil {
		t.Fatal("create table:", err)
	}

	for _, page := range pageTestData {
		if err := session.Query(`INSERT INTO page
			(title, revid, body, views, protected, modified, tags, attachments)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			page.Title, page.RevId, page.Body, page.Views, page.Protected,
			page.Modified, page.Tags, page.Attachments).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	var count int
	if err := session.Query("SELECT COUNT(*) FROM page").Scan(&count); err != nil {
		t.Error("select count:", err)
	}
	if count != len(pageTestData) {
		t.Errorf("count: expected %d, got %d\n", len(pageTestData), count)
	}

	for _, original := range pageTestData {
		page := new(Page)
		err := session.Query(`SELECT title, revid, body, views, protected, modified,
			tags, attachments
			FROM page WHERE title = ? AND revid = ? LIMIT 1`,
			original.Title, original.RevId).Scan(&page.Title, &page.RevId,
			&page.Body, &page.Views, &page.Protected, &page.Modified, &page.Tags,
			&page.Attachments)
		if err != nil {
			t.Error("select page:", err)
			continue
		}
		sort.Sort(sort.StringSlice(page.Tags))
		sort.Sort(sort.StringSlice(original.Tags))
		if !reflect.DeepEqual(page, original) {
			t.Errorf("page: expected %#v, got %#v\n", original, page)
		}
	}
}

func TestTracing(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE trace (id int primary key)`).Exec(); err != nil {
		t.Fatal("create:", err)
	}

	buf := &bytes.Buffer{}
	trace := NewTraceWriter(session, buf)

	if err := session.Query(`INSERT INTO trace (id) VALUES (?)`, 42).Trace(trace).Exec(); err != nil {
		t.Error("insert:", err)
	} else if buf.Len() == 0 {
		t.Error("insert: failed to obtain any tracing")
	}
	buf.Reset()

	var value int
	if err := session.Query(`SELECT id FROM trace WHERE id = ?`, 42).Trace(trace).Scan(&value); err != nil {
		t.Error("select:", err)
	} else if value != 42 {
		t.Errorf("value: expected %d, got %d", 42, value)
	} else if buf.Len() == 0 {
		t.Error("select: failed to obtain any tracing")
	}
}

func TestPaging(t *testing.T) {
	if *flagProto == 1 {
		t.Skip("Paging not supported. Please use Cassandra >= 2.0")
	}

	session := createSession(t)
	defer session.Close()

	if err := session.Query("CREATE TABLE large (id int primary key)").Exec(); err != nil {
		t.Fatal("create table:", err)
	}
	for i := 0; i < 100; i++ {
		if err := session.Query("INSERT INTO large (id) VALUES (?)", i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT id FROM large").PageSize(10).Iter()
	var id int
	count := 0
	for iter.Scan(&id) {
		count++
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 100 {
		t.Fatalf("expected %d, got %d", 100, count)
	}
}

func TestCAS(t *testing.T) {
	if *flagProto == 1 {
		t.Skip("lightweight transactions not supported. Please use Cassandra >= 2.0")
	}

	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE cas_table (
			title   varchar,
			revid   timeuuid,
			PRIMARY KEY (title, revid)
		)`).Exec(); err != nil {
		t.Fatal("create:", err)
	}

	title, revid := "baz", TimeUUID()
	var titleCAS string
	var revidCAS UUID

	if applied, err := session.Query(`INSERT INTO cas_table (title, revid)
		VALUES (?, ?) IF NOT EXISTS`,
		title, revid).ScanCAS(&titleCAS, &revidCAS); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatal("insert should have been applied")
	}

	if applied, err := session.Query(`INSERT INTO cas_table (title, revid)
		VALUES (?, ?) IF NOT EXISTS`,
		title, revid).ScanCAS(&titleCAS, &revidCAS); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatal("insert should not have been applied")
	} else if title != titleCAS || revid != revidCAS {
		t.Fatalf("expected %s/%v but got %s/%v", title, revid, titleCAS, revidCAS)
	}
}

func TestBatch(t *testing.T) {
	if *flagProto == 1 {
		t.Skip("atomic batches not supported. Please use Cassandra >= 2.0")
	}

	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE batch_table (id int primary key)`).Exec(); err != nil {
		t.Fatal("create table:", err)
	}

	batch := NewBatch(LoggedBatch)
	for i := 0; i < 100; i++ {
		batch.Query(`INSERT INTO batch_table (id) VALUES (?)`, i)
	}
	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal("execute batch:", err)
	}

	count := 0
	if err := session.Query(`SELECT COUNT(*) FROM batch_table`).Scan(&count); err != nil {
		t.Fatal("select count:", err)
	} else if count != 100 {
		t.Fatalf("count: expected %d, got %d\n", 100, count)
	}
}

type Page struct {
	Title       string
	RevId       UUID
	Body        string
	Views       int64
	Protected   bool
	Modified    time.Time
	Tags        []string
	Attachments map[string]Attachment
}

type Attachment []byte

var pageTestData = []*Page{
	&Page{
		Title:    "Frontpage",
		RevId:    TimeUUID(),
		Body:     "Welcome to this wiki page!",
		Modified: time.Date(2013, time.August, 13, 9, 52, 3, 0, time.UTC),
		Tags:     []string{"start", "important", "test"},
		Attachments: map[string]Attachment{
			"logo":    Attachment("\x00company logo\x00"),
			"favicon": Attachment("favicon.ico"),
		},
	},
	&Page{
		Title:    "Foobar",
		RevId:    TimeUUID(),
		Body:     "foo::Foo f = new foo::Foo(foo::Foo::INIT);",
		Modified: time.Date(2013, time.August, 13, 9, 52, 3, 0, time.UTC),
	},
}
