package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

type MyUDT struct {
	FieldA string `cql:"field_a"`
	FieldB int32 `cql:"field_b"`
}

// Example_userDefinedTypesStruct demonstrates how to work with user-defined types as structs.
// See also examples for UDTMarshaler and UDTUnmarshaler if you need more control/better performance.
func Example_userDefinedTypesStruct() {
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

	value := MyUDT{
		FieldA: "a value",
		FieldB: 42,
	}
	err = session.Query("INSERT INTO example.my_udt_table (pk, value) VALUES (?, ?)",
		1, value).WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}

	var readValue MyUDT

	err = session.Query("SELECT value FROM example.my_udt_table WHERE pk = 1").WithContext(ctx).Scan(&readValue)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(readValue.FieldA)
	fmt.Println(readValue.FieldB)
	// a value
	// 42
}
