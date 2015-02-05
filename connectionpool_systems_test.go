// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// +build conn_pool

package gocql

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// connection pool behavior test when nodes are removed from the cluster
// to run this test, see connectionpool_systems_test.sh

var (
	flagCluster  = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto    = flag.Int("proto", 2, "protcol version")
	flagCQL      = flag.String("cql", "3.0.0", "CQL version")
	flagRF       = flag.Int("rf", 1, "replication factor for test keyspace")
	clusterSize  = flag.Int("clusterSize", 1, "the expected size of the cluster")
	nodesShut    = flag.Int("nodesShut", 1, "the number of nodes to shutdown during the test")
	flagRetry    = flag.Int("retries", 5, "number of times to retry queries")
	flagRunSsl   = flag.Bool("runssl", false, "Set to true to run ssl test")
	clusterHosts []string
)
var initOnce sync.Once

func init() {
	flag.Parse()
	clusterHosts = strings.Split(*flagCluster, ",")
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func createTable(s *Session, table string) error {
	err := s.Query(table).Consistency(All).Exec()
	if *clusterSize > 1 {
		// wait for table definition to propogate
		time.Sleep(250 * time.Millisecond)
	}
	return err
}

func createCluster() *ClusterConfig {
	cluster := NewCluster(clusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = 5 * time.Second
	cluster.Consistency = Quorum
	if *flagRetry > 0 {
		cluster.RetryPolicy = &SimpleRetryPolicy{NumRetries: *flagRetry}
	}
	if *flagRunSsl {
		cluster.SslOpts = &SslOptions{
			CertPath:               "testdata/pki/gocql.crt",
			KeyPath:                "testdata/pki/gocql.key",
			CaPath:                 "testdata/pki/ca.crt",
			EnableHostVerification: false,
		}
	}
	return cluster
}

func createKeyspace(t testing.T, cluster *ClusterConfig, keyspace string) {
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("createSession:", err)
	}
	if err = session.Query(`DROP KEYSPACE ` + keyspace).Exec(); err != nil {
		t.Log("drop keyspace:", err)
	}
	err = session.Query(
		fmt.Sprintf(
			`
			CREATE KEYSPACE %s
			WITH replication = {
				'class' : 'SimpleStrategy',
				'replication_factor' : %d
			}
			`,
			keyspace,
			*flagRF,
		),
	).Consistency(All).Exec()
	if err != nil {
		t.Fatalf("error creating keyspace %s: %v", keyspace, err)
	}
	t.Logf("Created keyspace %s", keyspace)
	session.Close()
}

func createSession(t testing.T) *Session {
	cluster := createCluster()

	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		createKeyspace(t, cluster, "gocql_test")
	})

	cluster.Keyspace = "gocql_test"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal("createSession:", err)
	}

	return session
}

func TestSimplePool(t *testing.T) {
	testConnPool(t, NewSimplePool)
}

func TestRRPolicyConnPool(t *testing.T) {
	testConnPool(t, NewRoundRobinConnPool)
}

func TestTAPolicyConnPool(t *testing.T) {
	testConnPool(t, NewTokenAwareConnPool)
}

func testConnPool(t *testing.T, connPoolType func(*ClusterConfig) ConnectionPool) {
	var out []byte
	var err error
	log.SetFlags(log.Ltime)

	// make sure the cluster is running
	out, err = exec.Command("ccm", "start").CombinedOutput()
	if err != nil {
		t.Fatalf("Error running ccm command: %v", err)
		fmt.Printf("ccm output:\n%s", string(out))
	}

	time.Sleep(time.Duration(*clusterSize) * 1000 * time.Millisecond)

	// fire up a session (no discovery)
	cluster := createCluster()
	cluster.ConnPoolType = connPoolType
	cluster.DiscoverHosts = false
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("Error connecting to cluster: %v", err)
	}
	defer session.Close()

	time.Sleep(time.Duration(*clusterSize) * 1000 * time.Millisecond)

	if session.Pool.Size() != (*clusterSize)*cluster.NumConns {
		t.Errorf(
			"Expected %d pool size, but was %d",
			(*clusterSize)*cluster.NumConns,
			session.Pool.Size(),
		)
	}

	// start some connection monitoring
	nilCheckStop := false
	nilCount := 0
	nilCheck := func() {
		// assert that all connections returned by the pool are non-nil
		for !nilCheckStop {
			actual := session.Pool.Pick(nil)
			if actual == nil {
				nilCount++
			}
		}
	}
	go nilCheck()

	// shutdown some hosts
	log.Println("shutdown some hosts")
	for i := 0; i < *nodesShut; i++ {
		out, err = exec.Command("ccm", "node"+strconv.Itoa(i+1), "stop").CombinedOutput()
		if err != nil {
			t.Fatalf("Error running ccm command: %v", err)
			fmt.Printf("ccm output:\n%s", string(out))
		}
		time.Sleep(1500 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)

	if session.Pool.Size() != ((*clusterSize)-(*nodesShut))*cluster.NumConns {
		t.Errorf(
			"Expected %d pool size, but was %d",
			((*clusterSize)-(*nodesShut))*cluster.NumConns,
			session.Pool.Size(),
		)
	}

	// bringup the shutdown hosts
	log.Println("bringup the shutdown hosts")
	for i := 0; i < *nodesShut; i++ {
		out, err = exec.Command("ccm", "node"+strconv.Itoa(i+1), "start").CombinedOutput()
		if err != nil {
			t.Fatalf("Error running ccm command: %v", err)
			fmt.Printf("ccm output:\n%s", string(out))
		}
		time.Sleep(1500 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)

	if session.Pool.Size() != (*clusterSize)*cluster.NumConns {
		t.Errorf(
			"Expected %d pool size, but was %d",
			(*clusterSize)*cluster.NumConns,
			session.Pool.Size(),
		)
	}

	// assert that all connections returned by the pool are non-nil
	if nilCount > 0 {
		t.Errorf("%d nil connections returned from %T", nilCount, session.Pool)
	}

	// shutdown cluster
	log.Println("shutdown cluster")
	out, err = exec.Command("ccm", "stop").CombinedOutput()
	if err != nil {
		t.Fatalf("Error running ccm command: %v", err)
		fmt.Printf("ccm output:\n%s", string(out))
	}
	time.Sleep(2500 * time.Millisecond)

	if session.Pool.Size() != 0 {
		t.Errorf(
			"Expected %d pool size, but was %d",
			0,
			session.Pool.Size(),
		)
	}

	// start cluster
	log.Println("start cluster")
	out, err = exec.Command("ccm", "start").CombinedOutput()
	if err != nil {
		t.Fatalf("Error running ccm command: %v", err)
		fmt.Printf("ccm output:\n%s", string(out))
	}
	time.Sleep(500 * time.Millisecond)

	// reset the count
	nilCount = 0

	time.Sleep(3000 * time.Millisecond)

	if session.Pool.Size() != (*clusterSize)*cluster.NumConns {
		t.Errorf(
			"Expected %d pool size, but was %d",
			(*clusterSize)*cluster.NumConns,
			session.Pool.Size(),
		)
	}

	// assert that all connections returned by the pool are non-nil
	if nilCount > 0 {
		t.Errorf("%d nil connections returned from %T", nilCount, session.Pool)
	}
	nilCheckStop = true
}
