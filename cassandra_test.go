// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bytes"
	"flag"
	"reflect"
	"sort"
	"speter.net/go/exp/math/dec/inf"
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
	cluster.Authenticator = PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	initOnce.Do(func() {
		session, err := cluster.CreateSession()
		if err != nil {
			t.Fatal("createSession:", err)
		}
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
		session.Close()
	})
	cluster.Keyspace = "gocql_test"
	session, err := cluster.CreateSession()
	if err != nil {
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

//TestUseStatementError checks to make sure the correct error is returned when the user tries to execute a use statement.
func TestUseStatementError(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.Query("USE gocql_test").Exec(); err != nil {
		if err != ErrUseStmt {
			t.Error("expected ErrUseStmt, got " + err.Error())
		}
	} else {
		t.Error("expected err, got nil.")
	}
}

//TestInvalidKeyspace checks that an invalid keyspace will return promptly and without a flood of connections
func TestInvalidKeyspace(t *testing.T) {
	cluster := NewCluster(strings.Split(*flagCluster, ",")...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Keyspace = "invalidKeyspace"
	session, err := cluster.CreateSession()
	if err != nil {
		if err != ErrNoConnectionsStarted {
			t.Errorf("Expected ErrNoConnections but got %v", err)
		}
	} else {
		session.Close() //Clean up the session
		t.Error("expected err, got nil.")
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
			rating      decimal,
			tags        set<varchar>,
			attachments map<varchar, text>,
			PRIMARY KEY (title, revid)
		)`).Exec(); err != nil {
		t.Fatal("create table:", err)
	}

	for _, page := range pageTestData {
		if err := session.Query(`INSERT INTO page
			(title, revid, body, views, protected, modified, rating, tags, attachments)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			page.Title, page.RevId, page.Body, page.Views, page.Protected,
			page.Modified, page.Rating, page.Tags, page.Attachments).Exec(); err != nil {
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
			tags, attachments, rating
			FROM page WHERE title = ? AND revid = ? LIMIT 1`,
			original.Title, original.RevId).Scan(&page.Title, &page.RevId,
			&page.Body, &page.Views, &page.Protected, &page.Modified, &page.Tags,
			&page.Attachments, &page.Rating)
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

	if err := session.Query("CREATE TABLE paging (id int primary key)").Exec(); err != nil {
		t.Fatal("create table:", err)
	}
	for i := 0; i < 100; i++ {
		if err := session.Query("INSERT INTO paging (id) VALUES (?)", i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT id FROM paging").PageSize(10).Iter()
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

// TestBatchLimit tests gocql to make sure batch operations larger than the maximum
// statement limit are not submitted to a cassandra node.
func TestBatchLimit(t *testing.T) {
	if *flagProto == 1 {
		t.Skip("atomic batches not supported. Please use Cassandra >= 2.0")
	}
	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE batch_table2 (id int primary key)`).Exec(); err != nil {
		t.Fatal("create table:", err)
	}

	batch := NewBatch(LoggedBatch)
	for i := 0; i < 65537; i++ {
		batch.Query(`INSERT INTO batch_table2 (id) VALUES (?)`, i)
	}
	if err := session.ExecuteBatch(batch); err != ErrTooManyStmts {
		t.Fatal("gocql attempted to execute a batch larger than the support limit of statements.")
	}

}

// TestCreateSessionTimeout tests to make sure the CreateSession function timeouts out correctly
// and prevents an infinite loop of connection retries.
func TestCreateSessionTimeout(t *testing.T) {
	go func() {
		<-time.After(2 * time.Second)
		t.Fatal("no startup timeout")
	}()
	c := NewCluster("127.0.0.1:1")
	_, err := c.CreateSession()

	if err == nil {
		t.Fatal("expected ErrNoConnectionsStarted, but no error was returned.")
	}
	if err != ErrNoConnectionsStarted {
		t.Fatalf("expected ErrNoConnectionsStarted, but received %v", err)
	}
}

type Page struct {
	Title       string
	RevId       UUID
	Body        string
	Views       int64
	Protected   bool
	Modified    time.Time
	Rating      *inf.Dec
	Tags        []string
	Attachments map[string]Attachment
}

type Attachment []byte

var rating, _ = inf.NewDec(0, 0).SetString("0.131")

var pageTestData = []*Page{
	&Page{
		Title:    "Frontpage",
		RevId:    TimeUUID(),
		Body:     "Welcome to this wiki page!",
		Rating:   rating,
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

func TestSliceMap(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if err := session.Query(`CREATE TABLE slice_map_table (
			testuuid       timeuuid PRIMARY KEY,
			testtimestamp  timestamp,
			testvarchar    varchar,
			testbigint     bigint,
			testblob       blob,
			testbool       boolean,
			testfloat	   float,
			testdouble	   double,
			testint        int,
			testdecimal    decimal,
			testset        set<int>,
			testmap        map<varchar, varchar>
		)`).Exec(); err != nil {
		t.Fatal("create table:", err)
	}
	m := make(map[string]interface{})
	m["testuuid"] = TimeUUID()
	m["testvarchar"] = "Test VarChar"
	m["testbigint"] = time.Now().Unix()
	m["testtimestamp"] = time.Now().Truncate(time.Millisecond).UTC()
	m["testblob"] = []byte("test blob")
	m["testbool"] = true
	m["testfloat"] = float32(4.564)
	m["testdouble"] = float64(4.815162342)
	m["testint"] = 2343
	m["testdecimal"] = inf.NewDec(100, 0)
	m["testset"] = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	m["testmap"] = map[string]string{"field1": "val1", "field2": "val2", "field3": "val3"}
	sliceMap := []map[string]interface{}{m}
	if err := session.Query(`INSERT INTO slice_map_table (testuuid, testtimestamp, testvarchar, testbigint, testblob, testbool, testfloat, testdouble, testint, testdecimal, testset, testmap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m["testuuid"], m["testtimestamp"], m["testvarchar"], m["testbigint"], m["testblob"], m["testbool"], m["testfloat"], m["testdouble"], m["testint"], m["testdecimal"], m["testset"], m["testmap"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if returned, retErr := session.Query(`SELECT * FROM slice_map_table`).Iter().SliceMap(); retErr != nil {
		t.Fatal("select:", retErr)
	} else {
		if sliceMap[0]["testuuid"] != returned[0]["testuuid"] {
			t.Fatal("returned testuuid did not match")
		}
		if sliceMap[0]["testtimestamp"] != returned[0]["testtimestamp"] {
			t.Fatalf("returned testtimestamp did not match: %v %v", sliceMap[0]["testtimestamp"], returned[0]["testtimestamp"])
		}
		if sliceMap[0]["testvarchar"] != returned[0]["testvarchar"] {
			t.Fatal("returned testvarchar did not match")
		}
		if sliceMap[0]["testbigint"] != returned[0]["testbigint"] {
			t.Fatal("returned testbigint did not match")
		}
		if !reflect.DeepEqual(sliceMap[0]["testblob"], returned[0]["testblob"]) {
			t.Fatal("returned testblob did not match")
		}
		if sliceMap[0]["testbool"] != returned[0]["testbool"] {
			t.Fatal("returned testbool did not match")
		}
		if sliceMap[0]["testfloat"] != returned[0]["testfloat"] {
			t.Fatal("returned testfloat did not match")
		}
		if sliceMap[0]["testdouble"] != returned[0]["testdouble"] {
			t.Fatal("returned testdouble did not match")
		}
		if sliceMap[0]["testint"] != returned[0]["testint"] {
			t.Fatal("returned testint did not match")
		}

		expectedDecimal := sliceMap[0]["testdecimal"].(*inf.Dec)
		returnedDecimal := returned[0]["testdecimal"].(*inf.Dec)

		if expectedDecimal.Cmp(returnedDecimal) != 0 {
			t.Fatal("returned testdecimal did not match")
		}
		if !reflect.DeepEqual(sliceMap[0]["testset"], returned[0]["testset"]) {
			t.Fatal("returned testset did not match")
		}
		if !reflect.DeepEqual(sliceMap[0]["testmap"], returned[0]["testmap"]) {
			t.Fatal("returned testmap did not match")
		}
	}

	// Test for MapScan()
	testMap := make(map[string]interface{})
	if !session.Query(`SELECT * FROM slice_map_table`).Iter().MapScan(testMap) {
		t.Fatal("MapScan failed to work with one row")
	}
	if sliceMap[0]["testuuid"] != testMap["testuuid"] {
		t.Fatal("returned testuuid did not match")
	}
	if sliceMap[0]["testtimestamp"] != testMap["testtimestamp"] {
		t.Fatal("returned testtimestamp did not match")
	}
	if sliceMap[0]["testvarchar"] != testMap["testvarchar"] {
		t.Fatal("returned testvarchar did not match")
	}
	if sliceMap[0]["testbigint"] != testMap["testbigint"] {
		t.Fatal("returned testbigint did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testblob"], testMap["testblob"]) {
		t.Fatal("returned testblob did not match")
	}
	if sliceMap[0]["testbool"] != testMap["testbool"] {
		t.Fatal("returned testbool did not match")
	}
	if sliceMap[0]["testfloat"] != testMap["testfloat"] {
		t.Fatal("returned testfloat did not match")
	}
	if sliceMap[0]["testdouble"] != testMap["testdouble"] {
		t.Fatal("returned testdouble did not match")
	}
	if sliceMap[0]["testint"] != testMap["testint"] {
		t.Fatal("returned testint did not match")
	}

	expectedDecimal := sliceMap[0]["testdecimal"].(*inf.Dec)
	returnedDecimal := testMap["testdecimal"].(*inf.Dec)

	if expectedDecimal.Cmp(returnedDecimal) != 0 {
		t.Fatal("returned testdecimal did not match")
	}

	if !reflect.DeepEqual(sliceMap[0]["testset"], testMap["testset"]) {
		t.Fatal("returned testset did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testmap"], testMap["testmap"]) {
		t.Fatal("returned testmap did not match")
	}

}

func TestScanWithNilArguments(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE scan_with_nil_arguments (
			foo   varchar,
			bar   int,
			PRIMARY KEY (foo, bar)
	)`).Exec(); err != nil {
		t.Fatal("create:", err)
	}
	for i := 1; i <= 20; i++ {
		if err := session.Query("INSERT INTO scan_with_nil_arguments (foo, bar) VALUES (?, ?)",
			"squares", i*i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT * FROM scan_with_nil_arguments WHERE foo = ?", "squares").Iter()
	var n int
	count := 0
	for iter.Scan(nil, &n) {
		count += n
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 2870 {
		t.Fatalf("expected %d, got %d", 2870, count)
	}
}

func TestScanCASWithNilArguments(t *testing.T) {
	if *flagProto == 1 {
		t.Skip("lightweight transactions not supported. Please use Cassandra >= 2.0")
	}

	session := createSession(t)
	defer session.Close()

	if err := session.Query(`CREATE TABLE scan_cas_with_nil_arguments (
		foo   varchar,
		bar   varchar,
		PRIMARY KEY (foo, bar)
	)`).Exec(); err != nil {
		t.Fatal("create:", err)
	}

	foo := "baz"
	var cas string

	if applied, err := session.Query(`INSERT INTO scan_cas_with_nil_arguments (foo, bar)
		VALUES (?, ?) IF NOT EXISTS`,
		foo, foo).ScanCAS(nil, nil); err != nil {
		t.Fatal("insert:", err)
	} else if !applied {
		t.Fatal("insert should have been applied")
	}

	if applied, err := session.Query(`INSERT INTO scan_cas_with_nil_arguments (foo, bar)
		VALUES (?, ?) IF NOT EXISTS`,
		foo, foo).ScanCAS(&cas, nil); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatal("insert should not have been applied")
	} else if foo != cas {
		t.Fatalf("expected %v but got %v", foo, cas)
	}

	if applied, err := session.Query(`INSERT INTO scan_cas_with_nil_arguments (foo, bar)
		VALUES (?, ?) IF NOT EXISTS`,
		foo, foo).ScanCAS(nil, &cas); err != nil {
		t.Fatal("insert:", err)
	} else if applied {
		t.Fatal("insert should not have been applied")
	} else if foo != cas {
		t.Fatalf("expected %v but got %v", foo, cas)
	}
}

func injectInvalidPreparedStatement(t *testing.T, session *Session, table string) (string, *Conn) {
	if err := session.Query(`CREATE TABLE ` + table + ` (
			foo   varchar,
			bar   int,
			PRIMARY KEY (foo, bar)
	)`).Exec(); err != nil {
		t.Fatal("create:", err)
	}
	stmt := "INSERT INTO " + table + " (foo, bar) VALUES (?, 7)"
	conn := session.Pool.Pick(nil)
	conn.prepMu.Lock()
	flight := new(inflightPrepare)
	conn.prep[stmt] = flight
	flight.info = &queryInfo{
		id: []byte{'f', 'o', 'o', 'b', 'a', 'r'},
		args: []ColumnInfo{ColumnInfo{
			Keyspace: "gocql_test",
			Table:    table,
			Name:     "foo",
			TypeInfo: &TypeInfo{
				Type: TypeVarchar,
			},
		}, ColumnInfo{
			Keyspace: "gocql_test",
			Table:    table,
			Name:     "bar",
			TypeInfo: &TypeInfo{
				Type: TypeInt,
			},
		}},
	}
	conn.prepMu.Unlock()
	return stmt, conn
}

func TestReprepareStatement(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	stmt, conn := injectInvalidPreparedStatement(t, session, "test_reprepare_statement")
	query := session.Query(stmt, "bar")
	if err := conn.executeQuery(query).Close(); err != nil {
		t.Fatalf("Failed to execute query for reprepare statement: %v", err)
	}
}

func TestReprepareBatch(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	stmt, conn := injectInvalidPreparedStatement(t, session, "test_reprepare_statement_batch")
	batch := session.NewBatch(UnloggedBatch)
	batch.Query(stmt, "bar")
	if err := conn.executeBatch(batch); err != nil {
		t.Fatalf("Failed to execute query for reprepare statement: %v", err)
	}

}
