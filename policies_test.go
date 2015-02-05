// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import "testing"

func TestRoundRobinHostPolicy(t *testing.T) {
	policy := NewRoundRobinHostPolicy()

	hosts := []*HostInfo{
		&HostInfo{HostId: "0"},
		&HostInfo{HostId: "1"},
	}

	policy.SetHosts(hosts, "")

	// the first host selected is actually at [1], but this is ok for RR
	iter := policy.Pick(nil)
	if actual := iter(); actual != hosts[1] {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actual.HostId)
	}
	if actual := iter(); actual != hosts[0] {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actual.HostId)
	}
	iter = policy.Pick(nil)
	if actual := iter(); actual != hosts[0] {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actual.HostId)
	}
	if actual := iter(); actual != hosts[1] {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actual.HostId)
	}
	iter = policy.Pick(nil)
	if actual := iter(); actual != hosts[1] {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actual.HostId)
	}
	if actual := iter(); actual != hosts[0] {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actual.HostId)
	}
}

func TestTokenAwareHostPolicy(t *testing.T) {
	policy := NewTokenAwareHostPolicy(NewRoundRobinHostPolicy())

	hosts := []*HostInfo{
		&HostInfo{HostId: "0", Peer: "0", Tokens: []string{"00"}},
		&HostInfo{HostId: "1", Peer: "1", Tokens: []string{"25"}},
		&HostInfo{HostId: "2", Peer: "2", Tokens: []string{"50"}},
		&HostInfo{HostId: "3", Peer: "3", Tokens: []string{"75"}},
	}

	policy.SetHosts(hosts, "OrderedPartitioner")

	query := &Query{}
	query.RoutingKey([]byte("30"))

	if actual := policy.Pick(query)(); actual != hosts[2] {
		t.Errorf("Expected hosts[2] but was hosts[%s]", actual.HostId)
	}
}

func TestRoundRobinConnPolicy(t *testing.T) {
	policy := NewRoundRobinConnPolicy()

	conn0 := &Conn{}
	conn1 := &Conn{}
	conn := []*Conn{
		conn0,
		conn1,
	}

	policy.SetConns(conn)

	// the first conn selected is actually at [1], but this is ok for RR
	if actual := policy.Pick(nil); actual != conn1 {
		t.Error("Expected conn1")
	}
	if actual := policy.Pick(nil); actual != conn0 {
		t.Error("Expected conn0")
	}
	if actual := policy.Pick(nil); actual != conn1 {
		t.Error("Expected conn1")
	}
}
