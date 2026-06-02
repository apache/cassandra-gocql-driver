//go:build all || unit
// +build all unit

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
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestHostInfo_Lookup(t *testing.T) {
	hostLookupPreferV4 = true
	defer func() { hostLookupPreferV4 = false }()

	tests := [...]struct {
		addr string
		ip   net.IP
	}{
		{"127.0.0.1", net.IPv4(127, 0, 0, 1)},
		{"localhost", net.IPv4(127, 0, 0, 1)}, // TODO: this may be host dependant
	}

	for i, test := range tests {
		hosts, err := hostInfo(test.addr, 1)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		host := hosts[0]
		if !host.ConnectAddress().Equal(test.ip) {
			t.Errorf("expected ip %v got %v for addr %q", test.ip, host.ConnectAddress(), test.addr)
		}
	}
}

// blockingDNSResolver is a DNSResolver that sends a signal on `entered` on the
// first call and blocks the return until `unblock` is closed. It is used to
// turn the fallback path in controlConn.attemptReconnect into an "endless" one
// — which lets us detect whether HandleError blocks on reconnect synchronously
// or launches it in a goroutine.
type blockingDNSResolver struct {
	entered chan<- struct{}
	unblock <-chan struct{}
}

func (b *blockingDNSResolver) LookupIP(host string) ([]net.IP, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.unblock
	return nil, fmt.Errorf("blocked")
}

// TestControlConn_HandleError_LaunchesReconnectInGoroutine verifies that
// HandleError launches reconnect in a background goroutine, not synchronously.
// Regression test for scylladb/gocql#521 (deadlock in connection storm): a
// synchronous reconnect could hold the controlConn mutex while attemptReconnect
// did DNS resolution / TCP dial.
func TestControlConn_HandleError_LaunchesReconnectInGoroutine(t *testing.T) {
	resolverEntered := make(chan struct{}, 1)
	unblock := make(chan struct{})
	defer close(unblock)

	resolver := &blockingDNSResolver{
		entered: resolverEntered,
		unblock: unblock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &Session{
		cfg: ClusterConfig{
			DNSResolver: resolver,
			Hosts:       []string{"nonexistent.invalid"},
			Port:        9042,
		},
		ctx:    ctx,
		logger: newTestLogger(LogLevelDebug),
	}

	cc := createControlConn(session)

	fakeConn := &Conn{host: &HostInfo{}}

	start := time.Now()
	done := make(chan struct{})
	go func() {
		cc.HandleError(fakeConn, errors.New("boom"), true)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("HandleError did not return within 2s; it likely runs reconnect synchronously")
	}

	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("HandleError took %v, expected <100ms (sync reconnect bug?)", elapsed)
	}

	select {
	case <-resolverEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect goroutine never reached DNS resolution; was it started at all?")
	}
}
