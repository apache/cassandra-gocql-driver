// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bytes"
	"database/sql"
	"github.com/tux21b/gocql/uuid"
	"testing"
	"time"
)

func TestSimple(t *testing.T) {
	db, err := sql.Open("gocql", "localhost:9042 keyspace=system")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT keyspace_name FROM schema_keyspaces")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var keyspace string
		if err := rows.Scan(&keyspace); err != nil {
			t.Fatal(err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

type Page struct {
	Title      string
	RevID      uuid.UUID
	Body       string
	Hits       int
	Protected  bool
	Modified   time.Time
	Attachment []byte
}

var pages = []*Page{
	&Page{"Frontpage", uuid.TimeUUID(), "Hello world!", 0, false,
		time.Date(2012, 8, 20, 10, 0, 0, 0, time.UTC), nil},
	&Page{"Frontpage", uuid.TimeUUID(), "Hello modified world!", 0, false,
		time.Date(2012, 8, 22, 10, 0, 0, 0, time.UTC), []byte("img data\x00")},
	&Page{"LoremIpsum", uuid.TimeUUID(), "Lorem ipsum dolor sit amet", 12,
		true, time.Date(2012, 8, 22, 10, 0, 8, 0, time.UTC), nil},
}

func TestWiki(t *testing.T) {
	db, err := sql.Open("gocql", "localhost:9042 compression=snappy")
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("DROP KEYSPACE gocql_wiki")
	if _, err := db.Exec(`CREATE KEYSPACE gocql_wiki
	                      WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("USE gocql_wiki"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE page (
        title varchar,
        revid timeuuid,
        body varchar,
        hits int,
        protected boolean,
        modified timestamp,
        attachment blob,
        PRIMARY KEY (title, revid)
        )`); err != nil {
		t.Fatal(err)
	}
	for _, p := range pages {
		if _, err := db.Exec(`INSERT INTO page (title, revid, body, hits,
            protected, modified, attachment) VALUES (?, ?, ?, ?, ?, ?, ?);`,
			p.Title, p.RevID, p.Body, p.Hits, p.Protected, p.Modified,
			p.Attachment); err != nil {
			t.Fatal(err)
		}
	}

	row := db.QueryRow(`SELECT count(*) FROM page`)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Error(err)
	}
	if count != len(pages) {
		t.Fatalf("expected %d rows, got %d", len(pages), count)
	}

	for _, page := range pages {
		row := db.QueryRow(`SELECT title, revid, body, hits, protected,
            modified, attachment
            FROM page WHERE title = ? AND revid = ?`, page.Title, page.RevID)
		var p Page
		err := row.Scan(&p.Title, &p.RevID, &p.Body, &p.Hits, &p.Protected,
			&p.Modified, &p.Attachment)
		if err != nil {
			t.Fatal(err)
		}
		p.Modified = p.Modified.In(time.UTC)
		if page.Title != p.Title || page.RevID != p.RevID ||
			page.Body != p.Body || page.Modified != p.Modified ||
			page.Hits != p.Hits || page.Protected != p.Protected ||
			!bytes.Equal(page.Attachment, p.Attachment) {
			t.Errorf("expected %#v got %#v", *page, p)
		}
	}

	row = db.QueryRow(`SELECT title, revid, body, hits, protected,
        modified, attachment
        FROM page WHERE title = ? ORDER BY revid DESC`, "Frontpage")
	var p Page
	if err := row.Scan(&p.Title, &p.RevID, &p.Body, &p.Hits, &p.Protected,
		&p.Modified, &p.Attachment); err != nil {
		t.Error(err)
	}
	p.Modified = p.Modified.In(time.UTC)
	page := pages[1]
	if page.Title != p.Title || page.RevID != p.RevID ||
		page.Body != p.Body || page.Modified != p.Modified ||
		page.Hits != p.Hits || page.Protected != p.Protected ||
		!bytes.Equal(page.Attachment, p.Attachment) {
		t.Errorf("expected %#v got %#v", *page, p)
	}

}

func TestTypes(t *testing.T) {
	db, err := sql.Open("gocql", "localhost:9042 compression=snappy")
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("DROP KEYSPACE gocql_types")
	if _, err := db.Exec(`CREATE KEYSPACE gocql_types
	                      WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("USE gocql_types"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE stuff (
        id bigint,
		foo text,
        PRIMARY KEY (id)
        )`); err != nil {
		t.Fatal(err)
	}

	id := int64(-1 << 63)

	if _, err := db.Exec(`INSERT INTO stuff (id, foo) VALUES (?, ?);`, &id, "test"); err != nil {
		t.Fatal(err)
	}

	var rid int64

	row := db.QueryRow(`SELECT id FROM stuff WHERE id = ?`, id)

	if err := row.Scan(&rid); err != nil {
		t.Error(err)
	}

	if id != rid {
		t.Errorf("expected %v got %v", id, rid)
	}
}


func TestNullColumnValues(t *testing.T) {
	db, err := sql.Open("gocql", "localhost:9042 compression=snappy")
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("DROP KEYSPACE gocql_nullvalues")
	if _, err := db.Exec(`CREATE KEYSPACE gocql_nullvalues
	                      WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("USE gocql_nullvalues"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE stuff (
        id bigint,
        subid bigint,
				foo text,
				bar text,
        PRIMARY KEY (id, subid)
        )`); err != nil {
		t.Fatal(err)
	}
	id := int64(-1 << 63)

	if _, err := db.Exec(`INSERT INTO stuff (id, subid, foo) VALUES (?, ?, ?);`, id, int64(4), "test"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`INSERT INTO stuff (id, subid, bar) VALUES (?, ?, ?);`, id, int64(6), "test2"); err != nil {
		t.Fatal(err)
	}

	var rid int64
	var sid int64
	var data1 []byte
	var data2 []byte
	if rows, err := db.Query(`SELECT id, subid, foo, bar FROM stuff`); err == nil {
			for rows.Next() {
					if err := rows.Scan(&rid, &sid, &data1, &data2); err != nil {
				t.Error(err)
			}
		}
	} else {
		t.Fatal(err)
	}
}
