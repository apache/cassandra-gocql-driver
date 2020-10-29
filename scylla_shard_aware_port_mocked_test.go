// +build unit

package gocql

import (
	"context"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const testShardCount = 3

func TestShardAwarePortMockedNoReconnections(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortNoReconnections)
}

func TestShardAwarePortMockedMaliciousNAT(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortMaliciousNAT)
}

func TestShardAwarePortMockedUnreachable(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortUnreachable)
}

func TestShardAwarePortMockedUnusedIfNotEnabled(t *testing.T) {
	testWithAndWithoutTLS(t, testShardAwarePortUnusedIfNotEnabled)
}

func testWithAndWithoutTLS(t *testing.T, test func(t *testing.T, makeCluster makeClusterTestFunc)) {
	t.Run("without TLS", func(t *testing.T) {
		makeCluster, stop := startServerWithShardAwarePort(t, false)
		defer stop()
		test(t, makeCluster)
	})

	t.Run("with TLS", func(t *testing.T) {
		makeCluster, stop := startServerWithShardAwarePort(t, true)
		defer stop()
		test(t, makeCluster)
	})
}

func startServerWithShardAwarePort(t testing.TB, useTLS bool) (makeCluster func() *ClusterConfig, stop func()) {
	var shardAwarePort uint32

	shardAwarePortKey := "SCYLLA_SHARD_AWARE_PORT"
	if useTLS {
		shardAwarePortKey = "SCYLLA_SHARD_AWARE_PORT_SSL"
	}

	regularSupportedFactory := func(conn net.Conn) map[string][]string {
		// Assign a random shard. Although Scylla uses a deterministic algorithm
		// for assigning shards, the driver doesn't have enough information
		// to determine which shard will be assigned - therefore, from its
		// perspective, it's practically random.
		saPort := int(atomic.LoadUint32(&shardAwarePort))

		t.Log("Connecting to the regular port")

		shardID := rand.Intn(testShardCount)

		supported := getStandardScyllaExtensions(shardID, testShardCount)
		supported[shardAwarePortKey] = []string{strconv.Itoa(saPort)}
		return supported
	}

	shardAwareSupportedFactory := func(conn net.Conn) map[string][]string {
		// Shard ID depends on the source port.
		saPort := int(atomic.LoadUint32(&shardAwarePort))

		t.Log("Connecting to the shard-aware port")

		port := mustParsePortFromAddr(conn.RemoteAddr().String())
		shardID := scyllaShardForSourcePort(port, testShardCount)

		supported := getStandardScyllaExtensions(shardID, testShardCount)
		supported[shardAwarePortKey] = []string{strconv.Itoa(saPort)}
		return supported
	}

	makeServer := func(factory testSupportedFactory) *TestServer {
		if useTLS {
			return NewSSLTestServerWithSupportedFactory(t,
				defaultProto, context.Background(), factory)
		}
		return NewTestServerWithAddressAndSupportedFactory("127.0.0.1:0", t,
			defaultProto, context.Background(), factory)
	}

	srvRegular := makeServer(regularSupportedFactory)
	srvShardAware := makeServer(shardAwareSupportedFactory)

	saPort := mustParsePortFromAddr(srvShardAware.Address)
	atomic.StoreUint32(&shardAwarePort, uint32(saPort))

	t.Logf("regular port address: %s, shard aware port address: %s",
		srvRegular.Address, srvShardAware.Address)

	makeCluster = func() *ClusterConfig {
		var cluster *ClusterConfig
		if useTLS {
			cluster = createTestSslCluster(srvRegular.Address, defaultProto, false)

			// Give a long timeout. For some reason, closing tls connections
			// result in an i/o timeout error, and this mitigates this problem.
			cluster.Timeout = 1 * time.Minute
		} else {
			cluster = testCluster(defaultProto, srvRegular.Address)
		}
		return cluster
	}

	stop = func() {
		srvRegular.Stop()
		srvShardAware.Stop()
	}

	return makeCluster, stop
}

func mustParsePortFromAddr(addr string) uint16 {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		panic(err)
	}
	return uint16(port)
}

func getStandardScyllaExtensions(shardID, shardCount int) map[string][]string {
	return map[string][]string{
		"SCYLLA_SHARD":               []string{strconv.Itoa(shardID)},
		"SCYLLA_NR_SHARDS":           []string{strconv.Itoa(shardCount)},
		"SCYLLA_PARTITIONER":         []string{"org.apache.cassandra.dht.Murmur3Partitioner"},
		"SCYLLA_SHARDING_ALGORITHM":  []string{"biased-token-round-robin"},
		"SCYLLA_SHARDING_IGNORE_MSB": []string{"12"},
	}
}
