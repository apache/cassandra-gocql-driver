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
	"fmt"
	"sync"
	"testing"
	"time"
)

type TestHostFilter struct {
	mu           sync.Mutex
	allowedHosts map[string]*tcNode
}

func (f *TestHostFilter) Accept(h *HostInfo) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.allowedHosts[h.ConnectAddress().String()]
	return ok
}

func (f *TestHostFilter) SetAllowedHosts(hosts map[string]*tcNode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowedHosts = hosts
}

func TestControlConn_ReconnectRefreshesRing(t *testing.T) {
	ctx := context.Background()

	if len(cassNodes) < 2 {
		t.Skip("this test requires at least 2 nodes")
	}

	allAllowedHosts := map[string]*tcNode{}
	for _, node := range cassNodes {
		allAllowedHosts[node.Addr] = node
	}

	firstNode := cassNodes["node1"]
	allowedHosts := map[string]*tcNode{
		firstNode.Addr: firstNode,
	}

	testFilter := &TestHostFilter{allowedHosts: allowedHosts}

	session := createSession(t, func(config *ClusterConfig) {
		config.Hosts = []string{firstNode.Addr}
		config.Events.DisableTopologyEvents = true
		config.Events.DisableNodeStatusEvents = true
		config.HostFilter = testFilter
	})
	defer session.Close()

	if session.control == nil || session.control.conn.Load() == nil {
		t.Fatal("control conn is nil")
	}

	controlConnection := session.control.getConn()
	ccHost := controlConnection.host

	var ccHostName string
	for name, node := range cassNodes {
		if node.Addr == ccHost.ConnectAddress().String() {
			ccHostName = name
			break
		}
	}

	if ccHostName == "" {
		t.Fatal("could not find name of control host")
	}

	if err := cassNodes[ccHostName].TC.Stop(ctx, nil); err != nil {
		t.Fatal()
	}

	defer func(ctx context.Context) {
		if err := restoreCluster(ctx); err != nil {
			t.Fatalf("couldn't restore a cluster : %v", err)
		}
	}(ctx)

	assertNodeDown := func() error {
		hosts := session.ring.currentHosts()
		if len(hosts) != 1 {
			return fmt.Errorf("expected 1 host in ring but there were %v", len(hosts))
		}
		for _, host := range hosts {
			if host.IsUp() {
				return fmt.Errorf("expected host to be DOWN but %v isn't", host.String())
			}
		}

		session.pool.mu.RLock()
		poolsLen := len(session.pool.hostConnPools)
		session.pool.mu.RUnlock()
		if poolsLen != 0 {
			return fmt.Errorf("expected 0 connection pool but there were %v", poolsLen)
		}
		return nil
	}

	maxAttempts := 5
	delayPerAttempt := 1 * time.Second
	assertErr := assertNodeDown()
	for i := 0; i < maxAttempts && assertErr != nil; i++ {
		time.Sleep(delayPerAttempt)
		assertErr = assertNodeDown()
	}

	if assertErr != nil {
		t.Fatal(assertErr)
	}

	testFilter.SetAllowedHosts(allAllowedHosts)

	if err := restoreCluster(ctx); err != nil {
		t.Fatal(err)
	}

	assertNodeUp := func() error {
		hosts := session.ring.currentHosts()
		if len(hosts) != len(cassNodes) {
			return fmt.Errorf("expected %v hosts in ring but there were %v", len(ccHostName), len(hosts))
		}
		for _, host := range hosts {
			if !host.IsUp() {
				return fmt.Errorf("expected all hosts to be UP but %v isn't", host.String())
			}
		}
		session.pool.mu.RLock()
		poolsLen := len(session.pool.hostConnPools)
		session.pool.mu.RUnlock()
		if poolsLen != len(cassNodes) {
			return fmt.Errorf("expected %v connection pool but there were %v", len(ccHostName), poolsLen)
		}
		return nil
	}

	maxAttempts = 30
	delayPerAttempt = 1 * time.Second
	assertErr = assertNodeUp()
	for i := 0; i < maxAttempts && assertErr != nil; i++ {
		time.Sleep(delayPerAttempt)
		assertErr = assertNodeUp()
	}

	if assertErr != nil {
		t.Fatal(assertErr)
	}
}
