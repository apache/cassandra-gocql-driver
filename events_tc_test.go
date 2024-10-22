//go:build tc
// +build tc

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"testing"
	"time"
)

func TestEventDiscovery(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// check we discovered all the nodes in the ring
	for _, node := range cassNodes {
		host := session.ring.getHost(node.HostID)
		if host == nil {
			t.Errorf("did not discover %q", node.Addr)
		}
		if t.Failed() {
			t.FailNow()
		}
	}
}

func TestEventNodeDownControl(t *testing.T) {
	ctx := context.Background()
	const targetNode = "node1"

	node := cassNodes[targetNode]
	cluster := createCluster()
	cluster.Hosts = []string{node.Addr}
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	t.Logf("marking node %q down \n", targetNode)
	if err := node.TC.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	defer func(ctx context.Context) {
		if err := restoreCluster(ctx); err != nil {
			t.Fatalf("couldn't restore a cluster : %v", err)
		}
	}(ctx)

	if _, ok := getPool(session.pool, node.HostID); ok {
		t.Fatal("node not removed after remove event")
	}

	host := session.ring.getHost(node.HostID)
	if host == nil {
		t.Fatal("node not in metadata ring")
	} else if host.IsUp() {
		t.Fatalf("not not marked as down after event in metadata: %v", host)
	}
}

func TestEventNodeDown(t *testing.T) {
	ctx := context.Background()

	const targetNode = "node3"
	node := cassNodes[targetNode]

	session := createSession(t)
	defer session.Close()

	if err := node.TC.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	defer func(ctx context.Context) {
		if err := restoreCluster(ctx); err != nil {
			t.Fatalf("couldn't restore a cluster : %v", err)
		}
	}(ctx)

	if _, ok := getPool(session.pool, node.HostID); ok {
		t.Errorf("node not removed after remove event")
	}

	host := session.ring.getHost(node.HostID)
	if host == nil {
		t.Fatal("node not in metadata ring")
	} else if host.IsUp() {
		t.Fatalf("not not marked as down after event in metadata: %v", host)
	}
}

func TestEventNodeUp(t *testing.T) {
	ctx := context.Background()

	session := createSession(t)
	defer session.Close()

	const targetNode = "node2"
	node := cassNodes[targetNode]

	if _, ok := getPool(session.pool, node.HostID); !ok {
		t.Errorf("target pool not in connection pool: addr=%q pools=%v", node.Addr, session.pool.hostConnPools)
		t.FailNow()
	}

	if err := node.TC.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if _, ok := getPool(session.pool, node.HostID); ok {
		t.Fatal("node not removed after remove event")
	}

	if err := restoreCluster(ctx); err != nil {
		t.Fatalf("couldn't restore a cluster : %v", err)
	}

	// cassandra < 2.2 needs 10 seconds to start up the binary service
	time.Sleep(15 * time.Second)

	if _, ok := getPool(session.pool, node.HostID); !ok {
		t.Fatal("node not added after node added event")
	}

	host := session.ring.getHost(node.HostID)
	if host == nil {
		t.Fatal("node not in metadata ring")
	} else if !host.IsUp() {
		t.Fatalf("not not marked as UP after event in metadata: addr=%q host=%p: %v", node.Addr, host, host)
	}
}

func TestEventFilter(t *testing.T) {
	ctx := context.Background()

	cluster := createCluster()

	whiteListedNodeName := "node1"
	whiteListedNode := cassNodes[whiteListedNodeName]
	cluster.HostFilter = WhiteListHostFilter(whiteListedNode.Addr)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if _, ok := getPool(session.pool, whiteListedNode.HostID); !ok {
		t.Errorf("should have %v in pool but dont", whiteListedNodeName)
	}

	for _, node := range [...]string{"node2", "node3"} {
		if _, ok := getPool(session.pool, cassNodes[node].HostID); ok {
			t.Errorf("should not have %v in pool", node)
		}
	}

	if t.Failed() {
		t.FailNow()
	}

	shutdownNode := cassNodes["node2"]

	if err := shutdownNode.TC.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if err := restoreCluster(ctx); err != nil {
		t.Fatalf("couldn't restore a cluster : %v", err)
	}

	for _, node := range [...]string{"node2", "node3"} {
		_, ok := getPool(session.pool, cassNodes[node].HostID)
		if ok {
			t.Errorf("should not have %v in pool", node)
		}
	}

	if t.Failed() {
		t.FailNow()
	}
}

func TestEventDownQueryable(t *testing.T) {
	ctx := context.Background()

	targetNode := cassNodes["node1"]

	cluster := createCluster()
	cluster.Hosts = []string{targetNode.Addr}
	cluster.HostFilter = WhiteListHostFilter(targetNode.Addr)
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if pool, ok := getPool(session.pool, targetNode.HostID); !ok {
		t.Fatalf("should have %v in pool but dont", targetNode.Addr)
	} else if !pool.host.IsUp() {
		t.Fatalf("host is not up %v", pool.host)
	}

	if err := targetNode.TC.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if err := restoreCluster(ctx); err != nil {
		t.Fatalf("couldn't preserve a cluster : %v", err)
	}

	if pool, ok := getPool(session.pool, targetNode.HostID); !ok {
		t.Fatalf("should have %v in pool but dont", targetNode.Addr)
	} else if !pool.host.IsUp() {
		t.Fatalf("host is not up %v", pool.host)
	}

	var rows int
	if err := session.Query("SELECT COUNT(*) FROM system.local").Scan(&rows); err != nil {
		t.Fatal(err)
	} else if rows != 1 {
		t.Fatalf("expected to get 1 row got %d", rows)
	}
}
