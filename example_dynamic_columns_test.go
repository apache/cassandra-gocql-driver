package gocql_test

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
	"reflect"
	"text/tabwriter"
)

// Example_dynamicColumns demonstrates how to handle dynamic column list.
func Example_dynamicColumns() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.table1(pk text, ck int, value1 text, value2 int, PRIMARY KEY(pk, ck));
	insert into example.table1 (pk, ck, value1, value2) values ('a', 1, 'b', 2);
	insert into example.table1 (pk, ck, value1, value2) values ('c', 3, 'd', 4);
	insert into example.table1 (pk, ck, value1, value2) values ('c', 5, null, null);
	create table example.table2(pk int, value1 timestamp, PRIMARY KEY(pk));
	insert into example.table2 (pk, value1) values (1, '2020-01-02 03:04:05');
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	printQuery := func(ctx context.Context, session *gocql.Session, stmt string, values ...interface{}) error {
		iter := session.Query(stmt, values...).WithContext(ctx).Iter()
		fmt.Println(stmt)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ',
			0)
		for i, columnInfo := range iter.Columns() {
			if i > 0 {
				fmt.Fprint(w, "\t| ")
			}
			fmt.Fprintf(w, "%s (%s)", columnInfo.Name, columnInfo.TypeInfo)
		}

		for {
			rd, err := iter.RowData()
			if err != nil {
				return err
			}
			if !iter.Scan(rd.Values...) {
				break
			}
			fmt.Fprint(w, "\n")
			for i, val := range rd.Values {
				if i > 0 {
					fmt.Fprint(w, "\t| ")
				}

				fmt.Fprint(w, reflect.Indirect(reflect.ValueOf(val)).Interface())
			}
		}

		fmt.Fprint(w, "\n")
		w.Flush()
		fmt.Println()

		return iter.Close()
	}

	ctx := context.Background()

	err = printQuery(ctx, session, "SELECT * FROM table1")
	if err != nil {
		log.Fatal(err)
	}

	err = printQuery(ctx, session, "SELECT value2, pk, ck FROM table1")
	if err != nil {
		log.Fatal(err)
	}

	err = printQuery(ctx, session, "SELECT * FROM table2")
	if err != nil {
		log.Fatal(err)
	}
	// SELECT * FROM table1
	// pk (varchar) | ck (int) | value1 (varchar) | value2 (int)
	// a            | 1        | b                | 2
	// c            | 3        | d                | 4
	// c            | 5        |                  | 0
	//
	// SELECT value2, pk, ck FROM table1
	// value2 (int) | pk (varchar) | ck (int)
	// 2            | a            | 1
	// 4            | c            | 3
	// 0            | c            | 5
	//
	// SELECT * FROM table2
	// pk (int) | value1 (timestamp)
	// 1        | 2020-01-02 03:04:05 +0000 UTC
}
