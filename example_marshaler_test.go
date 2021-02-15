package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"strconv"
	"strings"
)

// MyMarshaler implements Marshaler and Unmarshaler.
// It represents a version number stored as string.
type MyMarshaler struct {
	major, minor, patch int
}

func (m MyMarshaler) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return gocql.Marshal(info, fmt.Sprintf("%d.%d.%d", m.major, m.minor, m.patch))
}

func (m *MyMarshaler) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	var s string
	err := gocql.Unmarshal(info, data, &s)
	if err != nil {
		return err
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return fmt.Errorf("parse version %q: %d parts instead of 3", s, len(parts))
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("parse version %q major number: %v", s, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("parse version %q minor number: %v", s, err)
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("parse version %q patch number: %v", s, err)
	}
	m.major = major
	m.minor = minor
	m.patch = patch
	return nil
}

// Example_marshalerUnmarshaler demonstrates how to implement a Marshaler and Unmarshaler.
func Example_marshalerUnmarshaler() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.my_marshaler_table(pk int, value text, PRIMARY KEY(pk));
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

	value := MyMarshaler{
		major: 1,
		minor: 2,
		patch: 3,
	}
	err = session.Query("INSERT INTO example.my_marshaler_table (pk, value) VALUES (?, ?)",
		1, value).WithContext(ctx).Exec()
	if err != nil {
		log.Fatal(err)
	}
	var stringValue string
	err = session.Query("SELECT value FROM example.my_marshaler_table WHERE pk = 1").WithContext(ctx).
		Scan(&stringValue)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(stringValue)
	var unmarshaledValue MyMarshaler
	err = session.Query("SELECT value FROM example.my_marshaler_table WHERE pk = 1").WithContext(ctx).
		Scan(&unmarshaledValue)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(unmarshaledValue)
	// 1.2.3
	// {1 2 3}
}
