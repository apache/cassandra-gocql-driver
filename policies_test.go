// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hailocab/go-hostpool"
)

// Tests of the round-robin host selection policy implementation
func TestRoundRobbin(t *testing.T) {
	policy := RoundRobinHostPolicy()

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(0, 0, 0, 1)},
		{hostId: "1", connectAddress: net.IPv4(0, 0, 0, 2)},
	}

	for _, host := range hosts {
		policy.AddHost(host)
	}

	got := make(map[string]bool)
	it := policy.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		if got[id] {
			t.Fatalf("got duplicate host: %v", id)
		}
		got[id] = true
	}
	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}
}

// Tests of the token-aware host selection policy implementation with a
// round-robin host selection policy fallback.
func TestHostPolicy_TokenAware_SimpleStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The SimpleStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("00"), []*HostInfo{hosts[0], hosts[1]}},
			{orderedToken("25"), []*HostInfo{hosts[1], hosts[2]}},
			{orderedToken("50"), []*HostInfo{hosts[2], hosts[3]}},
			{orderedToken("75"), []*HostInfo{hosts[3], hosts[0]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("20"))
	iter = policy.Pick(query)
	iterCheck(t, iter, "0")
	iterCheck(t, iter, "1")
}

// Tests of the host pool host selection policy implementation
func TestHostPolicy_HostPool(t *testing.T) {
	policy := HostPoolHostPolicy(hostpool.New(nil))

	hosts := []*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 0)},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 1)},
	}

	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.(*hostPoolHostPolicy).SetHosts(hosts)

	// the first host selected is actually at [1], but this is ok for RR
	// interleaved iteration should always increment the host
	iter := policy.Pick(nil)
	actualA := iter()
	if actualA.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualA.Info().HostID())
	}
	actualA.Mark(nil)

	actualB := iter()
	if actualB.Info().HostID() != "1" {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actualB.Info().HostID())
	}
	actualB.Mark(fmt.Errorf("error"))

	actualC := iter()
	if actualC.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualC.Info().HostID())
	}
	actualC.Mark(nil)

	actualD := iter()
	if actualD.Info().HostID() != "0" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualD.Info().HostID())
	}
	actualD.Mark(nil)
}

func TestHostPolicy_RoundRobin_NilHostInfo(t *testing.T) {
	policy := RoundRobinHostPolicy()

	host := &HostInfo{hostId: "host-1"}
	policy.AddHost(host)

	iter := policy.Pick(nil)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if v.HostID() != host.HostID() {
		t.Fatalf("expected host %v got %v", host, v)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestHostPolicy_TokenAware_NilHostInfo(t *testing.T) {
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return "myKeyspace" }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	hosts := [...]*HostInfo{
		{connectAddress: net.IPv4(10, 0, 0, 0), tokens: []string{"00"}},
		{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"25"}},
		{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"50"}},
		{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"75"}},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	query := &Query{}
	query.getKeyspace = func() string { return "myKeyspace" }
	query.RoutingKey([]byte("20"))

	iter := policy.Pick(query)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if !v.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
		t.Fatalf("expected peer 1 got %v", v.ConnectAddress())
	}

	// Empty the hosts to trigger the panic when using the fallback.
	for _, host := range hosts {
		policy.RemoveHost(host)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestCOWList_Add(t *testing.T) {
	var cow cowHostList

	toAdd := [...]net.IP{net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2), net.IPv4(10, 0, 0, 3)}

	for _, addr := range toAdd {
		if !cow.add(&HostInfo{connectAddress: addr}) {
			t.Fatal("did not add peer which was not in the set")
		}
	}

	hosts := cow.get()
	if len(hosts) != len(toAdd) {
		t.Fatalf("expected to have %d hosts got %d", len(toAdd), len(hosts))
	}

	set := make(map[string]bool)
	for _, host := range hosts {
		set[string(host.ConnectAddress())] = true
	}

	for _, addr := range toAdd {
		if !set[string(addr)] {
			t.Errorf("addr was not in the host list: %q", addr)
		}
	}
}

// TestSimpleRetryPolicy makes sure that we only allow 1 + numRetries attempts
func TestSimpleRetryPolicy(t *testing.T) {
	q := &Query{}

	// this should allow a total of 3 tries.
	rt := &SimpleRetryPolicy{NumRetries: 2}

	cases := []struct {
		attempts int
		allow    bool
	}{
		{0, true},
		{1, true},
		{2, true},
		{3, false},
		{4, false},
		{5, false},
	}

	for _, c := range cases {
		q.metrics = preFilledQueryMetrics(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

func TestExponentialBackoffPolicy(t *testing.T) {
	// test with defaults
	sut := &ExponentialBackoffRetryPolicy{NumRetries: 2}

	cases := []struct {
		attempts int
		delay    time.Duration
	}{

		{1, 100 * time.Millisecond},
		{2, (2) * 100 * time.Millisecond},
		{3, (2 * 2) * 100 * time.Millisecond},
		{4, (2 * 2 * 2) * 100 * time.Millisecond},
	}
	for _, c := range cases {
		// test 100 times for each case
		for i := 0; i < 100; i++ {
			d := sut.napTime(c.attempts)
			if d < c.delay-(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d less than jitter min of %d", d, c.delay-100*time.Millisecond/2)
			}
			if d > c.delay+(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d greater than jitter max of %d", d, c.delay+100*time.Millisecond/2)
			}
		}
	}
}

func TestDowngradingConsistencyRetryPolicy(t *testing.T) {

	q := &Query{cons: LocalQuorum}

	rewt0 := &RequestErrWriteTimeout{
		Received:  0,
		WriteType: "SIMPLE",
	}

	rewt1 := &RequestErrWriteTimeout{
		Received:  1,
		WriteType: "BATCH",
	}

	rewt2 := &RequestErrWriteTimeout{
		WriteType: "UNLOGGED_BATCH",
	}

	rert := &RequestErrReadTimeout{}

	reu0 := &RequestErrUnavailable{
		Alive: 0,
	}

	reu1 := &RequestErrUnavailable{
		Alive: 1,
	}

	// this should allow a total of 3 tries.
	consistencyLevels := []Consistency{Three, Two, One}
	rt := &DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: consistencyLevels}
	cases := []struct {
		attempts  int
		allow     bool
		err       error
		retryType RetryType
	}{
		{0, true, rewt0, Rethrow},
		{3, true, rewt1, Ignore},
		{1, true, rewt2, Retry},
		{2, true, rert, Retry},
		{4, false, reu0, Rethrow},
		{16, false, reu1, Retry},
	}

	for _, c := range cases {
		q.metrics = preFilledQueryMetrics(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.retryType != rt.GetRetryType(c.err) {
			t.Fatalf("retry type should be %v", c.retryType)
		}
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

func iterCheck(t *testing.T, iter NextHost, hostID string) {
	t.Helper()

	host := iter()
	if host == nil || host.Info() == nil {
		t.Fatalf("expected hostID %s got nil", hostID)
	}
	if host.Info().HostID() != hostID {
		t.Fatalf("Expected peer %s but was %s", hostID, host.Info().HostID())
	}
}

func TestHostPolicy_DCAwareRR(t *testing.T) {
	p := DCAwareRoundRobinPolicy("local")

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "remote"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "remote"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	got := make(map[string]bool, len(hosts))
	var dcs []string

	it := p.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		dc := h.Info().dataCenter

		if got[id] {
			t.Fatalf("got duplicate host %s", id)
		}
		got[id] = true
		dcs = append(dcs, dc)
	}

	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}

	var remote bool
	for _, dc := range dcs {
		if dc == "local" {
			if remote {
				t.Fatalf("got local dc after remote: %v", dcs)
			}
		} else {
			remote = true
		}
	}

}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 1, "b": 1, "c": 1} replication.
func TestHostPolicy_TokenAware(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"))
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(query)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   1,
				"remote1": 1,
				"remote2": 1,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("23"))
	iter = policy.Pick(query)
	// first should be host with matching token from the local DC
	iterCheck(t, iter, "4")
	// next are in non deterministic order
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAware_NetworkStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"), NonLocalReplicasFallback())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	policyInternal.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   2,
				"remote1": 2,
				"remote2": 2,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		keyspace: {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0], hosts[1], hosts[2]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2], hosts[3], hosts[4]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("23"))
	iter = policy.Pick(query)
	// first should be hosts with matching token from the local DC
	iterCheck(t, iter, "4")
	iterCheck(t, iter, "7")
	// rest should be hosts with matching token from remote DCs
	iterCheck(t, iter, "3")
	iterCheck(t, iter, "5")
	iterCheck(t, iter, "6")
	iterCheck(t, iter, "8")
}
