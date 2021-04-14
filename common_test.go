package gocql

import (
	"flag"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	flagCluster      = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto        = flag.Int("proto", 0, "protcol version")
	flagCQL          = flag.String("cql", "3.0.0", "CQL version")
	flagRF           = flag.Int("rf", 1, "replication factor for test keyspace")
	clusterSize      = flag.Int("clusterSize", 1, "the expected size of the cluster")
	flagRetry        = flag.Int("retries", 5, "number of times to retry queries")
	flagAutoWait     = flag.Duration("autowait", 1000*time.Millisecond, "time to wait for autodiscovery to fill the hosts poll")
	flagRunSslTest   = flag.Bool("runssl", false, "Set to true to run ssl test")
	flagRunAuthTest  = flag.Bool("runauth", false, "Set to true to run authentication test")
	flagCompressTest = flag.String("compressor", "", "compressor to use")
	flagTimeout      = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")

	flagCassVersion cassVersion
)

func init() {
	flag.Var(&flagCassVersion, "gocql.cversion", "the cassandra version being tested against")

	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func getClusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}

func addSslOptions(cluster *ClusterConfig) *ClusterConfig {
	if *flagRunSslTest {
		cluster.SslOpts = &SslOptions{
			CertPath:               "testdata/pki/gocql.crt",
			KeyPath:                "testdata/pki/gocql.key",
			CaPath:                 "testdata/pki/ca.crt",
			EnableHostVerification: false,
		}
	}
	return cluster
}

var initOnce sync.Once

func createTable(s *Session, table string) error {
	// lets just be really sure
	if err := s.control.awaitSchemaAgreement(); err != nil {
		log.Printf("error waiting for schema agreement pre create table=%q err=%v\n", table, err)
		return err
	}

	if err := s.Query(table).RetryPolicy(&SimpleRetryPolicy{}).Exec(); err != nil {
		log.Printf("error creating table table=%q err=%v\n", table, err)
		return err
	}

	if err := s.control.awaitSchemaAgreement(); err != nil {
		log.Printf("error waiting for schema agreement post create table=%q err=%v\n", table, err)
		return err
	}

	return nil
}

func createCluster(opts ...func(*ClusterConfig)) *ClusterConfig {
	clusterHosts := getClusterHosts()
	cluster := NewCluster(clusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	switch *flagCompressTest {
	case "snappy":
		cluster.Compressor = &SnappyCompressor{}
	case "":
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}

	cluster = addSslOptions(cluster)

	for _, opt := range opts {
		opt(cluster)
	}

	return cluster
}

func createKeyspace(tb testing.TB, cluster *ClusterConfig, keyspace string) {
	// TODO: tb.Helper()
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = createTable(session, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		panic(fmt.Sprintf("unable to drop keyspace: %v", err))
	}

	err = createTable(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))

	if err != nil {
		panic(fmt.Sprintf("unable to create keyspace: %v", err))
	}
}

func createSessionFromCluster(cluster *ClusterConfig, tb testing.TB) *Session {
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		createKeyspace(tb, cluster, "gocql_test")
	})

	cluster.Keyspace = "gocql_test"
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
	}

	if err := session.control.awaitSchemaAgreement(); err != nil {
		tb.Fatal(err)
	}

	return session
}

func createSession(tb testing.TB, opts ...func(config *ClusterConfig)) *Session {
	cluster := createCluster(opts...)
	return createSessionFromCluster(cluster, tb)
}

func createViews(t *testing.T, session *Session) {
	if err := session.Query(`
		CREATE TYPE IF NOT EXISTS gocql_test.basicView (
		birthday timestamp,
		nationality text,
		weight text,
		height text);	`).Exec(); err != nil {
		t.Fatalf("failed to create view with err: %v", err)
	}
}

func createMaterializedViews(t *testing.T, session *Session) {
	if flagCassVersion.Before(3, 0, 0) {
		return
	}
	if err := session.Query(`CREATE TABLE IF NOT EXISTS gocql_test.view_table (
		    userid text,
		    year int,
		    month int,
    		    PRIMARY KEY (userid));`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(`CREATE TABLE IF NOT EXISTS gocql_test.view_table2 (
		    userid text,
		    year int,
		    month int,
    		    PRIMARY KEY (userid));`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(`CREATE MATERIALIZED VIEW IF NOT EXISTS gocql_test.view_view AS
		   SELECT year, month, userid
		   FROM gocql_test.view_table
		   WHERE year IS NOT NULL AND month IS NOT NULL AND userid IS NOT NULL
		   PRIMARY KEY (userid, year);`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
	if err := session.Query(`CREATE MATERIALIZED VIEW IF NOT EXISTS gocql_test.view_view2 AS
		   SELECT year, month, userid
		   FROM gocql_test.view_table2
		   WHERE year IS NOT NULL AND month IS NOT NULL AND userid IS NOT NULL
		   PRIMARY KEY (userid, year);`).Exec(); err != nil {
		t.Fatalf("failed to create materialized view with err: %v", err)
	}
}

func createFunctions(t *testing.T, session *Session) {
	if err := session.Query(`
		CREATE OR REPLACE FUNCTION gocql_test.avgState ( state tuple<int,bigint>, val int )
		CALLED ON NULL INPUT
		RETURNS tuple<int,bigint>
		LANGUAGE java AS
		$$if (val !=null) {state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue());}return state;$$;	`).Exec(); err != nil {
		t.Fatalf("failed to create function with err: %v", err)
	}
	if err := session.Query(`
		CREATE OR REPLACE FUNCTION gocql_test.avgFinal ( state tuple<int,bigint> )
		CALLED ON NULL INPUT
		RETURNS double
		LANGUAGE java AS
		$$double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);$$ 
	`).Exec(); err != nil {
		t.Fatalf("failed to create function with err: %v", err)
	}
}

func createAggregate(t *testing.T, session *Session) {
	createFunctions(t, session)
	if err := session.Query(`
		CREATE OR REPLACE AGGREGATE gocql_test.average(int)
		SFUNC avgState
		STYPE tuple<int,bigint>
		FINALFUNC avgFinal
		INITCOND (0,0);
	`).Exec(); err != nil {
		t.Fatalf("failed to create aggregate with err: %v", err)
	}
	if err := session.Query(`
		CREATE OR REPLACE AGGREGATE gocql_test.average2(int)
		SFUNC avgState
		STYPE tuple<int,bigint>
		FINALFUNC avgFinal
		INITCOND (0,0);
	`).Exec(); err != nil {
		t.Fatalf("failed to create aggregate with err: %v", err)
	}
}

func staticAddressTranslator(newAddr net.IP, newPort int) AddressTranslator {
	return AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return newAddr, newPort
	})
}

func assertTrue(t *testing.T, description string, value bool) {
	t.Helper()
	if !value {
		t.Fatalf("expected %s to be true", description)
	}
}

func assertEqual(t *testing.T, description string, expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %s to be (%+v) but was (%+v) instead", description, expected, actual)
	}
}

func assertDeepEqual(t *testing.T, description string, expected, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %s to be (%+v) but was (%+v) instead", description, expected, actual)
	}
}

func assertNil(t *testing.T, description string, actual interface{}) {
	t.Helper()
	if actual != nil {
		t.Fatalf("expected %s to be (nil) but was (%+v) instead", description, actual)
	}
}
