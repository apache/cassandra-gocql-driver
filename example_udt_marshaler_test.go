package gocql_test

import (
	"context"
	"github.com/gocql/gocql"
	"log"
)

// MyUDTMarshaler implements UDTMarshaler.
type MyUDTMarshaler struct {
	fieldA string
	fieldB int32
}

// MarshalUDT marshals the selected field to bytes.
func (m MyUDTMarshaler) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "field_a":
		return gocql.Marshal(info, m.fieldA)
	case "field_b":
		return gocql.Marshal(info, m.fieldB)
	default:
		// If you want to be strict and return error un unknown field, you can do so here instead.
		// Returning nil, nil will set the value of unknown fields to null, which might be handy if you want
		// to be forward-compatible when a new field is added to the UDT.
		return nil, nil
	}
}

// ExampleUDTMarshaler demonstrates how to implement a UDTMarshaler.
func ExampleUDTMarshaler() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create type example.my_udt (field_a text, field_b int);
	create table example.my_udt_table(pk int, value frozen<my_udt>, PRIMARY KEY(pk));
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	value := MyUDTMarshaler{
		fieldA: "a value",
		fieldB: 42,
	}
	err = session.Query("INSERT INTO example.my_udt_table (pk, value) VALUES (?, ?)",
		1, value).WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}
}
