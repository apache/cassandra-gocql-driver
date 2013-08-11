package gocql

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	db := NewSession(Config{
		Nodes: []string{
			"127.0.0.1",
		},
		Keyspace:    "system",
		Consistency: ConQuorum,
	})
	defer db.Close()

	for i := 0; i < 5; i++ {
		db.Query("SELECT keyspace_name FROM schema_keyspaces WHERE keyspace_name = ?",
			"system_auth").Exec()
	}

	var keyspace string
	var durable bool
	iter := db.Query("SELECT keyspace_name, durable_writes FROM schema_keyspaces").Iter()
	for iter.Scan(&keyspace, &durable) {
		fmt.Println("Keyspace:", keyspace, durable)
	}
	if err := iter.Close(); err != nil {
		fmt.Println(err)
	}
}

type Page struct {
	Title      string
	RevID      int
	Body       string
	Hits       int
	Protected  bool
	Modified   time.Time
	Attachment []byte
}

var pages = []*Page{
	&Page{"Frontpage", 1, "Hello world!", 0, false,
		time.Date(2012, 8, 20, 10, 0, 0, 0, time.UTC), []byte{}},
	&Page{"Frontpage", 2, "Hello modified world!", 0, false,
		time.Date(2012, 8, 22, 10, 0, 0, 0, time.UTC), []byte("img data\x00")},
	&Page{"LoremIpsum", 3, "Lorem ipsum dolor sit amet", 12,
		true, time.Date(2012, 8, 22, 10, 0, 8, 0, time.UTC), []byte{}},
}

func TestWiki(t *testing.T) {
	db := NewSession(Config{
		Nodes:       []string{"localhost"},
		Consistency: ConQuorum,
	})

	if err := db.Query("DROP KEYSPACE gocql_wiki").Exec(); err != nil {
		t.Log("DROP KEYSPACE:", err)
	}

	if err := db.Query(`CREATE KEYSPACE gocql_wiki
		WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : 1
		}`).Exec(); err != nil {
		t.Fatal("CREATE KEYSPACE:", err)
	}

	if err := db.Query("USE gocql_wiki").Exec(); err != nil {
		t.Fatal("USE:", err)
	}

	if err := db.Query(`CREATE TABLE page (
		title varchar,
		revid int,
		body varchar,
		hits int,
		protected boolean,
		modified timestamp,
		attachment blob,
		PRIMARY KEY (title, revid)
		)`).Exec(); err != nil {
		t.Fatal("CREATE TABLE:", err)
	}

	for _, p := range pages {
		if err := db.Query(`INSERT INTO page (title, revid, body, hits,
			protected, modified, attachment) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			p.Title, p.RevID, p.Body, p.Hits, p.Protected, p.Modified,
			p.Attachment).Exec(); err != nil {
			t.Fatal("INSERT:", err)
		}
	}

	var count int
	if err := db.Query("SELECT count(*) FROM page").Scan(&count); err != nil {
		t.Fatal("COUNT:", err)
	}
	if count != len(pages) {
		t.Fatalf("COUNT: expected %d got %d", len(pages), count)
	}

	for _, page := range pages {
		qry := db.Query(`SELECT title, revid, body, hits, protected,
			modified, attachment
		    FROM page WHERE title = ? AND revid = ?`, page.Title, page.RevID)
		var p Page
		if err := qry.Scan(&p.Title, &p.RevID, &p.Body, &p.Hits, &p.Protected,
			&p.Modified, &p.Attachment); err != nil {
			t.Fatal("SELECT PAGE:", err)
		}
		p.Modified = p.Modified.In(time.UTC)
		if page.Title != p.Title || page.RevID != p.RevID ||
			page.Body != p.Body || page.Modified != p.Modified ||
			page.Hits != p.Hits || page.Protected != p.Protected ||
			!bytes.Equal(page.Attachment, p.Attachment) {
			t.Errorf("expected %#v got %#v", *page, p)
		}
	}
}
