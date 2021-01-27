// +build all integration

package gocql

import (
	"reflect"
	"testing"
)

func TestTupleSimple(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}

	err := createTable(session, `CREATE TABLE gocql_test.tuple_test(
		id int,
		coord frozen<tuple<int, int>>,

		primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	err = session.Query("INSERT INTO tuple_test(id, coord) VALUES(?, (?, ?))", 1, 100, -100).Exec()
	if err != nil {
		t.Fatal(err)
	}

	var (
		id    int
		coord struct {
			x int
			y int
		}
	)

	iter := session.Query("SELECT id, coord FROM tuple_test WHERE id=?", 1)
	if err := iter.Scan(&id, &coord.x, &coord.y); err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Errorf("expected to get id=1 got: %v", id)
	} else if coord.x != 100 {
		t.Errorf("expected to get coord.x=100 got: %v", coord.x)
	} else if coord.y != -100 {
		t.Errorf("expected to get coord.y=-100 got: %v", coord.y)
	}

}

func TestTuple_NullTuple(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}

	err := createTable(session, `CREATE TABLE gocql_test.tuple_nil_test(
		id int,
		coord frozen<tuple<int, int>>,

		primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	const id = 1

	err = session.Query("INSERT INTO tuple_nil_test(id, coord) VALUES(?, (?, ?))", id, nil, nil).Exec()
	if err != nil {
		t.Fatal(err)
	}

	x := new(int)
	y := new(int)
	iter := session.Query("SELECT coord FROM tuple_nil_test WHERE id=?", id)
	if err := iter.Scan(&x, &y); err != nil {
		t.Fatal(err)
	}

	if x != nil {
		t.Fatalf("should be nil got %+#v", x)
	} else if y != nil {
		t.Fatalf("should be nil got %+#v", y)
	}

}

func TestTuple_TupleNotSet(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}

	err := createTable(session, `CREATE TABLE gocql_test.tuple_not_set_test(
		id int,
		coord frozen<tuple<int, int>>,

		primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	const id = 1

	err = session.Query("INSERT INTO tuple_not_set_test(id,coord) VALUES(?, (?,?))", id, 1, 2).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = session.Query("INSERT INTO tuple_not_set_test(id) VALUES(?)", id+1).Exec()
	if err != nil {
		t.Fatal(err)
	}

	x := new(int)
	y := new(int)
	iter := session.Query("SELECT coord FROM tuple_not_set_test WHERE id=?", id)
	if err := iter.Scan(x, y); err != nil {
		t.Fatal(err)
	}
	if x == nil || *x != 1 {
		t.Fatalf("x should be %d got %+#v, value=%d", 1, x, *x)
	}
	if y == nil || *y != 2 {
		t.Fatalf("y should be %d got %+#v, value=%d", 2, y, *y)
	}

	// Check if the supplied targets are reset to nil
	iter = session.Query("SELECT coord FROM tuple_not_set_test WHERE id=?", id+1)
	if err := iter.Scan(x, y); err != nil {
		t.Fatal(err)
	}
	if x == nil || *x != 0 {
		t.Fatalf("x should be %d got %+#v, value=%d", 0, x, *x)
	}
	if y == nil || *y != 0 {
		t.Fatalf("y should be %d got %+#v, value=%d", 0, y, *y)
	}
}

func TestTupleMapScan(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}

	err := createTable(session, `CREATE TABLE gocql_test.tuple_map_scan(
		id int,
		val frozen<tuple<int, int>>,

		primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	if err := session.Query(`INSERT INTO tuple_map_scan (id, val) VALUES (1, (1, 2));`).Exec(); err != nil {
		t.Fatal(err)
	}

	m := make(map[string]interface{})
	err = session.Query(`SELECT * FROM tuple_map_scan`).MapScan(m)
	if err != nil {
		t.Fatal(err)
	}
	if m["val[0]"] != 1 {
		t.Fatalf("expacted val[0] to be %d but was %d", 1, m["val[0]"])
	}
	if m["val[1]"] != 2 {
		t.Fatalf("expacted val[1] to be %d but was %d", 2, m["val[1]"])
	}
}

func TestTupleMapScanNil(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}
	err := createTable(session, `CREATE TABLE gocql_test.tuple_map_scan_nil(
			id int,
			val frozen<tuple<int, int>>,

			primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Query(`INSERT INTO tuple_map_scan_nil (id, val) VALUES (?,(?,?));`, 1, nil, nil).Exec(); err != nil {
		t.Fatal(err)
	}

	m := make(map[string]interface{})
	err = session.Query(`SELECT * FROM tuple_map_scan_nil`).MapScan(m)
	if err != nil {
		t.Fatal(err)
	}
	if m["val[0]"] != 0 {
		t.Fatalf("expacted val[0] to be %d but was %d", 0, m["val[0]"])
	}
	if m["val[1]"] != 0 {
		t.Fatalf("expacted val[1] to be %d but was %d", 0, m["val[1]"])
	}
}

func TestTupleMapScanNotSet(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}
	err := createTable(session, `CREATE TABLE gocql_test.tuple_map_scan_not_set(
			id int,
			val frozen<tuple<int, int>>,

			primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Query(`INSERT INTO tuple_map_scan_not_set (id) VALUES (?);`, 1).Exec(); err != nil {
		t.Fatal(err)
	}

	m := make(map[string]interface{})
	err = session.Query(`SELECT * FROM tuple_map_scan_not_set`).MapScan(m)
	if err != nil {
		t.Fatal(err)
	}
	if m["val[0]"] != 0 {
		t.Fatalf("expacted val[0] to be %d but was %d", 0, m["val[0]"])
	}
	if m["val[1]"] != 0 {
		t.Fatalf("expacted val[1] to be %d but was %d", 0, m["val[1]"])
	}
}

func TestTupleLastFieldEmpty(t *testing.T) {
	// Regression test - empty value used to be treated as NULL value in the last tuple field
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}
	err := createTable(session, `CREATE TABLE gocql_test.tuple_last_field_empty(
			id int,
			val frozen<tuple<text, text>>,

			primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	if err := session.Query(`INSERT INTO tuple_last_field_empty (id, val) VALUES (?,(?,?));`, 1, "abc", "").Exec(); err != nil {
		t.Fatal(err)
	}

	var e1, e2 *string
	if err := session.Query("SELECT val FROM tuple_last_field_empty WHERE id = ?", 1).Scan(&e1, &e2); err != nil {
		t.Fatal(err)
	}

	if e1 == nil {
		t.Fatal("expected e1 not to be nil")
	}
	if *e1 != "abc" {
		t.Fatalf("expected e1 to be equal to \"abc\", but is %v", *e2)
	}
	if e2 == nil {
		t.Fatal("expected e2 not to be nil")
	}
	if *e2 != "" {
		t.Fatalf("expected e2 to be an empty string, but is %v", *e2)
	}
}

func TestTuple_NestedCollection(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}

	err := createTable(session, `CREATE TABLE gocql_test.nested_tuples(
		id int,
		val list<frozen<tuple<int, text>>>,

		primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	type typ struct {
		A int
		B string
	}

	tests := []struct {
		name string
		val  interface{}
	}{
		{name: "slice", val: [][]interface{}{{1, "2"}, {3, "4"}}},
		{name: "array", val: [][2]interface{}{{1, "2"}, {3, "4"}}},
		{name: "struct", val: []typ{{1, "2"}, {3, "4"}}},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := session.Query(`INSERT INTO nested_tuples (id, val) VALUES (?, ?);`, i, test.val).Exec(); err != nil {
				t.Fatal(err)
			}

			rv := reflect.ValueOf(test.val)
			res := reflect.New(rv.Type()).Elem().Addr().Interface()

			err = session.Query(`SELECT val FROM nested_tuples WHERE id=?`, i).Scan(res)
			if err != nil {
				t.Fatal(err)
			}

			resVal := reflect.ValueOf(res).Elem().Interface()
			if !reflect.DeepEqual(test.val, resVal) {
				t.Fatalf("unmarshaled value not equal to the original value: expected %#v, got %#v", test.val, resVal)
			}
		})
	}
}

func TestTuple_NullableNestedCollection(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion3 {
		t.Skip("tuple types are only available of proto>=3")
	}

	err := createTable(session, `CREATE TABLE gocql_test.nested_tuples_with_nulls(
		id int,
		val list<frozen<tuple<text, text>>>,

		primary key(id))`)
	if err != nil {
		t.Fatal(err)
	}

	type typ struct {
		A *string
		B *string
	}

	ptrStr := func(s string) *string {
		ret := new(string)
		*ret = s
		return ret
	}

	tests := []struct {
		name string
		val  interface{}
	}{
		{name: "slice", val: [][]*string{{ptrStr("1"), nil}, {nil, ptrStr("2")}, {ptrStr("3"), ptrStr("")}}},
		{name: "array", val: [][2]*string{{ptrStr("1"), nil}, {nil, ptrStr("2")}, {ptrStr("3"), ptrStr("")}}},
		{name: "struct", val: []typ{{ptrStr("1"), nil}, {nil, ptrStr("2")}, {ptrStr("3"), ptrStr("")}}},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := session.Query(`INSERT INTO nested_tuples_with_nulls (id, val) VALUES (?, ?);`, i, test.val).Exec(); err != nil {
				t.Fatal(err)
			}

			rv := reflect.ValueOf(test.val)
			res := reflect.New(rv.Type()).Interface()

			err = session.Query(`SELECT val FROM nested_tuples_with_nulls WHERE id=?`, i).Scan(res)
			if err != nil {
				t.Fatal(err)
			}

			resVal := reflect.ValueOf(res).Elem().Interface()
			if !reflect.DeepEqual(test.val, resVal) {
				t.Fatalf("unmarshaled value not equal to the original value: expected %#v, got %#v", test.val, resVal)
			}
		})
	}
}
