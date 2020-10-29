// +build integration unit

package gocql

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

type makeClusterTestFunc func() *ClusterConfig

func testShardAwarePortNoReconnections(t *testing.T, makeCluster makeClusterTestFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Initialize 10 sessions in parallel.
	// If shard-aware port is used and configured properly, we should get
	// a connection to each shard without any retries.
	// For each host, there should be N-1 connections to the special port.

	// Run 10 sessions in parallel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each session gets a separate configuration, because we need to have
			// separate connection listeners - we need to differentiate connections
			// made for each session separately
			dialer := newLoggingTestDialer()
			cluster := makeCluster()
			cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
			cluster.Dialer = dialer

			useTLS := cluster.SslOpts != nil

			sess, err := cluster.CreateSession()
			if err != nil {
				cancel()
				return
			}
			defer sess.Close()

			if err := waitUntilPoolsStopFilling(ctx, sess, 10*time.Second); err != nil {
				cancel()
				return
			}

			hosts := sess.ring.allHosts()
			for _, host := range hosts {
				t.Logf("checking host %s", host.hostname)
				hostPool, _ := sess.pool.getPool(host)

				shardAwarePort := getShardAwarePort(hostPool, useTLS)
				if shardAwarePort == 0 {
					// Shard aware port was not exposed by the host
					t.Log("the host does not expose a shard-aware port, skipping")
					continue
				}

				// Verify that we have a sharded connPicker
				shardedPicker, ok := hostPool.connPicker.(*scyllaConnPicker)
				if !ok {
					t.Errorf("not a sharded connection")
					continue
				}

				numberOfShards := shardedPicker.nrShards

				// Verify that there were no duplicate connections to the same shard
				// Make sure that we didn't connect to the same shard twice
				// There should be numberOfShards-1 connections to the new port
				events := dialer.events[host.hostname]
				shardAwareConnectionCount := 0
				shardsConnected := make(map[int]testConnectionEvent)
				for _, evt := range events {
					if evt.destinationPort != shardAwarePort {
						continue
					}

					shardAwareConnectionCount++

					shard := scyllaShardForSourcePort(evt.sourcePort, numberOfShards)
					if oldPort, hasShard := shardsConnected[shard]; hasShard {
						t.Errorf("there was more than one connection to the shard aware port from the same shard (shard %d, port %d and %d)",
							shard, oldPort, evt.sourcePort)
					}
				}

				if shardAwareConnectionCount != numberOfShards-1 {
					t.Errorf("expected %d connections to the shard aware port, but got %d", numberOfShards-1, shardAwareConnectionCount)
				}
			}

			return
		}()
	}

	wg.Wait()
}

func testShardAwarePortMaliciousNAT(t *testing.T, makeCluster makeClusterTestFunc) {
	cluster := makeCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Dialer = &sourcePortOffByOneTestDialer{}

	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("an error occurred while creating a session: %s", err)
	}
	defer sess.Close()

	// In this situation we are guaranteed that the connection will miss one
	// shard at this point. The first connection receives a random shard,
	// then we establish N-1 connections, targeting remaining shards.
	// Because the malicious port translator shifts the port by one,
	// one shard will be missed (if the host has more than one shard).

	// Retry until we establish one connection per shard

	for {
		if err := waitUntilPoolsStopFilling(context.Background(), sess, 10*time.Second); err != nil {
			t.Fatal(err)
		}

		if checkIfPoolsAreFull(sess) {
			break
		}

		triggerPoolsRefill(sess)
	}
}

func testShardAwarePortUnreachable(t *testing.T, makeCluster makeClusterTestFunc) {
	cluster := makeCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	cluster.Dialer = &allowOnlyNonShardAwarePortDialer{allowedPort: getClusterPort(cluster)}

	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("an error occurred while creating a session: %s", err)
	}
	defer sess.Close()

	// In this situation, the connecting to the shard-aware port will fail,
	// but connections to the non-shard-aware port will succeed. This test
	// checks that we detect that the shard-aware-port is unreachable and
	// we fall back to the old port.

	// Retry until we establish one connection per shard

	for {
		if err := waitUntilPoolsStopFilling(context.Background(), sess, 10*time.Second); err != nil {
			t.Fatal(err)
		}

		if checkIfPoolsAreFull(sess) {
			break
		}

		triggerPoolsRefill(sess)
	}
}

func testShardAwarePortUnusedIfNotEnabled(t *testing.T, makeCluster makeClusterTestFunc) {
	dialer := newLoggingTestDialer()
	cluster := makeCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())

	// Explicitly disable the shard aware port
	cluster.DisableShardAwarePort = true
	cluster.Dialer = dialer

	useTLS := cluster.SslOpts != nil

	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("an error occurred while creating a session: %s", err)
	}
	defer sess.Close()

	if err := waitUntilPoolsStopFilling(context.Background(), sess, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	hosts := sess.ring.allHosts()
	for _, host := range hosts {
		t.Logf("checking host %s", host.hostname)
		hostPool, _ := sess.pool.getPool(host)

		shardAwarePort := getShardAwarePort(hostPool, useTLS)
		if shardAwarePort == 0 {
			// Shard aware port was not exposed by the host
			t.Log("the host does not expose a shard-aware port, skipping")
			continue
		}

		events, _ := dialer.events[host.hostname]

		for _, evt := range events {
			if evt.destinationPort == shardAwarePort {
				t.Error("there was an attempt to connect to a shard aware port, but the configuration does not allow that")
			}
		}
	}
}

func checkIfShardAwarePortIsExposed(pool *hostConnPool, useTLS bool) bool {
	return getShardAwareAddress(pool, useTLS) != ""
}

func getShardAwarePort(pool *hostConnPool, useTLS bool) uint16 {
	addr := getShardAwareAddress(pool, useTLS)
	if addr == "" {
		return 0
	}

	_, portS, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portS)
	return uint16(port)
}

func getShardAwareAddress(pool *hostConnPool, useTLS bool) string {
	picker, ok := pool.connPicker.(*scyllaConnPicker)
	if !ok {
		return ""
	}
	return picker.shardAwareAddress
}

func triggerPoolsRefill(sess *Session) {
	hosts := sess.ring.allHosts()
	for _, host := range hosts {
		hostPool, _ := sess.pool.getPool(host)
		go hostPool.fill()
	}
}

func waitUntilPoolsStopFilling(ctx context.Context, sess *Session, timeout time.Duration) error {
	deadline := time.After(timeout)
	for !checkIfPoolsStoppedFilling(sess) {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-deadline:
			return fmt.Errorf("failed to fill all connection pools in %s", timeout)

		case <-time.After(250 * time.Millisecond):
			continue
		}
	}

	return nil
}

func checkIfPoolsStoppedFilling(sess *Session) bool {
	hosts := sess.ring.allHosts()
	for _, host := range hosts {
		hostPool, _ := sess.pool.getPool(host)

		hostPool.mu.Lock()
		isFilling := hostPool.filling
		hostPool.mu.Unlock()

		if isFilling {
			return false
		}
	}

	return true
}

func checkIfPoolsAreFull(sess *Session) bool {
	hosts := sess.ring.allHosts()
	for _, host := range hosts {
		hostPool, _ := sess.pool.getPool(host)

		hostPool.mu.Lock()
		_, remaining := hostPool.connPicker.Size()
		hostPool.mu.Unlock()

		if remaining > 0 {
			return false
		}
	}

	return true
}

func getClusterPort(cluster *ClusterConfig) uint16 {
	_, portStr, _ := net.SplitHostPort(cluster.Hosts[0])
	port, _ := strconv.Atoi(portStr)

	if port == 0 {
		// Assume default if it's not explicitly specified
		return 9042
	}
	return uint16(port)
}

type sourcePortOffByOneTestDialer struct {
	ScyllaShardAwareDialer
}

func (spobo *sourcePortOffByOneTestDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Simulate a NAT that always increases the source port by 1
	// This should always result in wrong shard being assigned if host
	// has more than one shard.

	sourcePort := ScyllaGetSourcePort(ctx)
	if sourcePort > 0 {
		sourcePort++
	}
	newCtx := context.WithValue(ctx, scyllaSourcePortCtx{}, sourcePort)
	return spobo.ScyllaShardAwareDialer.DialContext(newCtx, network, addr)
}

type allowOnlyNonShardAwarePortDialer struct {
	net.Dialer

	allowedPort uint16
}

func (aonsa *allowOnlyNonShardAwarePortDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Simulate a network configuration which allows connections to the
	// non-shard-aware port, but not to the shard-aware one.

	_, targetPort, _ := net.SplitHostPort(addr)
	if targetPort != strconv.Itoa(int(aonsa.allowedPort)) {
		return nil, fmt.Errorf("allowOnlyNonShardAwarePortDialer: tried to connect to port %s, but only %d is allowed", targetPort, aonsa.allowedPort)
	}

	return aonsa.Dialer.DialContext(ctx, network, addr)
}

type loggingTestDialer struct {
	ScyllaShardAwareDialer

	events map[string][]testConnectionEvent
	mu     sync.Mutex
}

type testConnectionEvent struct {
	sourcePort, destinationPort uint16
}

func newLoggingTestDialer() *loggingTestDialer {
	return &loggingTestDialer{
		events: make(map[string][]testConnectionEvent),
	}
}

func (ltd *loggingTestDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	sourcePort := ScyllaGetSourcePort(ctx)

	hostname, destinationPortStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	destinationPort, err := strconv.ParseUint(destinationPortStr, 10, 16)
	if err != nil {
		return nil, err
	}

	conn, err := ltd.ScyllaShardAwareDialer.DialContext(ctx, network, addr)

	if err == nil {
		ltd.mu.Lock()
		defer ltd.mu.Unlock()

		evt := testConnectionEvent{
			sourcePort:      sourcePort,
			destinationPort: uint16(destinationPort),
		}
		ltd.events[hostname] = append(ltd.events[hostname], evt)
	}

	return conn, err
}
