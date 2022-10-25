package gocql

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

// Tests of the token-aware host selection policy implementation with a
// round-robin host selection policy fallback.
func TestClusterMetadataManager_SimpleStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	var mngr clusterMetadataManager
	mngr.getKeyspaceName = func() string { return keyspace }
	mngr.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}

	// set the hosts
	hosts := []*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
	}
	mngr.addHosts(hosts)
	mngr.setPartitioner("OrderedPartitioner")

	mngr.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
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
	mngr.keyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// The SimpleStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
			{orderedToken("00"), []*HostInfo{hosts[0], hosts[1]}},
			{orderedToken("25"), []*HostInfo{hosts[1], hosts[2]}},
			{orderedToken("50"), []*HostInfo{hosts[2], hosts[3]}},
			{orderedToken("75"), []*HostInfo{hosts[3], hosts[0]}},
		},
	}, mngr.getMetadataReadOnly().replicas)
}

func TestClusterMetadataManager_NilHostInfo(t *testing.T) {
	var mngr clusterMetadataManager
	mngr.getKeyspaceName = func() string { return "myKeyspace" }
	mngr.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	hosts := []*HostInfo{
		{connectAddress: net.IPv4(10, 0, 0, 0), tokens: []string{"00"}},
		{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"25"}},
		{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"50"}},
		{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"75"}},
	}
	mngr.addHosts(hosts)
	mngr.setPartitioner("OrderedPartitioner")

	// No replicas can be computed because we don't have keyspace metadata yet
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{}, mngr.getMetadataReadOnly().replicas)
}

// Tests of the cluster metadata manager implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 1, "b": 1, "c": 1} replication.
func TestClusterMetadataManager_NetworkTopologyStrategy_A1_B1_C1(t *testing.T) {
	const keyspace = "myKeyspace"
	var mngr clusterMetadataManager
	mngr.getKeyspaceName = func() string { return keyspace }
	mngr.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	// set the hosts
	hosts := []*HostInfo{
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
	mngr.addHosts(hosts)
	mngr.setPartitioner("OrderedPartitioner")

	mngr.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
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
	mngr.keyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

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
	}, mngr.getMetadataReadOnly().replicas)
}

// Tests of the cluster metadata manager implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestClusterMetadataManager_NetworkTopologyStrategy_A2_B2_C2(t *testing.T) {
	const keyspace = "myKeyspace"
	var mngr clusterMetadataManager
	mngr.getKeyspaceName = func() string { return keyspace }
	mngr.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	// set the hosts
	hosts := []*HostInfo{
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
	mngr.addHosts(hosts)
	mngr.setPartitioner("OrderedPartitioner")

	mngr.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
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
	mngr.keyspaceChanged(KeyspaceUpdateEvent{Keyspace: "myKeyspace"})

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		"myKeyspace": {
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
	}, mngr.getMetadataReadOnly().replicas)
}
