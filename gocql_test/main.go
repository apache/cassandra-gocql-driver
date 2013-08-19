// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"reflect"
	"sort"
	"time"

	"tux21b.org/v1/gocql"
	"tux21b.org/v1/gocql/uuid"
)

var session *gocql.Session

func init() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Compressor = gocql.SnappyCompressor{}
	session = cluster.CreateSession()
}

type Page struct {
	Title       string
	RevId       uuid.UUID
	Body        string
	Views       int64
	Protected   bool
	Modified    time.Time
	Tags        []string
	Attachments map[string]Attachment
}

type Attachment []byte

func initSchema() error {
	if err := session.Query("DROP KEYSPACE gocql_test").Exec(); err != nil {
		log.Println("drop keyspace", err)
	}

	if err := session.Query(`CREATE KEYSPACE gocql_test
		WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : 1
		}`).Exec(); err != nil {
		return err
	}

	if err := session.Query("USE gocql_test").Exec(); err != nil {
		return err
	}

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
		return err
	}

	if err := session.Query(`CREATE TABLE page_stats (
			title varchar,
			views counter,
			PRIMARY KEY (title)
		)`).Exec(); err != nil {
		return err
	}

	return nil
}

var pageTestData = []*Page{
	&Page{
		Title:    "Frontpage",
		RevId:    uuid.TimeUUID(),
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
		RevId:    uuid.TimeUUID(),
		Body:     "foo::Foo f = new foo::Foo(foo::Foo::INIT);",
		Modified: time.Date(2013, time.August, 13, 9, 52, 3, 0, time.UTC),
	},
}

func insertTestData() error {
	for _, page := range pageTestData {
		if err := session.Query(`INSERT INTO page
			(title, revid, body, views, protected, modified, tags, attachments)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			page.Title, page.RevId, page.Body, page.Views, page.Protected,
			page.Modified, page.Tags, page.Attachments).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func insertBatch() error {
	batch := gocql.NewBatch(gocql.LoggedBatch)
	for _, page := range pageTestData {
		batch.Query(`INSERT INTO page
			(title, revid, body, views, protected, modified, tags, attachments)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			page.Title, page.RevId, page.Body, page.Views, page.Protected,
			page.Modified, page.Tags, page.Attachments)
	}
	if err := session.ExecuteBatch(batch); err != nil {
		return err
	}
	return nil
}

func getPage(title string, revid uuid.UUID) (*Page, error) {
	p := new(Page)
	err := session.Query(`SELECT title, revid, body, views, protected, modified,
			tags, attachments
			FROM page WHERE title = ? AND revid = ? LIMIT 1`, title, revid).Scan(
		&p.Title, &p.RevId, &p.Body, &p.Views, &p.Protected, &p.Modified,
		&p.Tags, &p.Attachments)
	return p, err
}

func main() {
	if err := initSchema(); err != nil {
		log.Fatal("initSchema: ", err)
	}

	if err := insertTestData(); err != nil {
		log.Fatal("insertTestData: ", err)
	}

	var count int
	if err := session.Query("SELECT COUNT(*) FROM page").Scan(&count); err != nil {
		log.Fatal("getCount: ", err)
	}
	if count != len(pageTestData) {
		log.Printf("count: expected %d, got %d", len(pageTestData), count)
	}

	for _, original := range pageTestData {
		page, err := getPage(original.Title, original.RevId)
		if err != nil {
			log.Print("getPage: ", err)
			continue
		}
		sort.Sort(sort.StringSlice(page.Tags))
		sort.Sort(sort.StringSlice(original.Tags))
		if !reflect.DeepEqual(page, original) {
			log.Printf("page: expected %#v, got %#v\n", original, page)
		}
	}

	if err := session.Query("CREATE TABLE large (id int primary key)").Exec(); err != nil {
		log.Fatal("create table", err)
	}
	for i := 0; i < 100; i++ {
		if err := session.Query("INSERT INTO large (id) VALUES (?)", i).Exec(); err != nil {
			log.Fatal("insert", err)
		}
	}
	iter := session.Query("SELECT id FROM large").PageSize(10).Iter()
	var id int
	count = 0
	for iter.Scan(&id) {
		count++
	}
	if err := iter.Close(); err != nil {
		log.Fatal("large iter:", err)
	}
	if count != 100 {
		log.Fatalf("expected %d, got %d", 100, count)
	}

	for _, original := range pageTestData {
		if err := session.Query("DELETE FROM page WHERE title = ? AND revid = ?",
			original.Title, original.RevId).Exec(); err != nil {
			log.Println("delete:", err)
		}
	}
	if err := session.Query("SELECT COUNT(*) FROM page").Scan(&count); err != nil {
		log.Fatal("getCount: ", err)
	}
	if count != 0 {
		log.Printf("count: expected %d, got %d", len(pageTestData), count)
	}

	if err := insertBatch(); err != nil {
		log.Fatal("insertBatch: ", err)
	}

}
