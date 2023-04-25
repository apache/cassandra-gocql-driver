//go:build ccm
// +build ccm

package gocql

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/internal/ccm"
)

type TestHostFilter struct {
	mu           sync.Mutex
	allowedHosts map[string]ccm.Host
}

func (f *TestHostFilter) Accept(h *HostInfo) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.allowedHosts[h.ConnectAddress().String()]
	return ok
}

func (f *TestHostFilter) SetAllowedHosts(hosts map[string]ccm.Host) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowedHosts = hosts
}

func TestControlConn_ReconnectRefreshesRing(t *testing.T) {
	if err := ccm.AllUp(); err != nil {
		t.Fatal(err)
	}

	allCcmHosts, err := ccm.Status()
	if err != nil {
		t.Fatal(err)
	}

	if len(allCcmHosts) < 2 {
		t.Skip("this test requires at least 2 nodes")
	}

	allAllowedHosts := map[string]ccm.Host{}
	var firstNode *ccm.Host
	for _, node := range allCcmHosts {
		if firstNode == nil {
			firstNode = &node
		}
		allAllowedHosts[node.Addr] = node
	}

	allowedHosts := map[string]ccm.Host{
		firstNode.Addr: *firstNode,
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
	for _, node := range allCcmHosts {
		if node.Addr == ccHost.ConnectAddress().String() {
			ccHostName = node.Name
			break
		}
	}

	if ccHostName == "" {
		t.Fatal("could not find name of control host")
	}

	if err := ccm.NodeDown(ccHostName); err != nil {
		t.Fatal()
	}

	defer func() {
		ccmStatus, err := ccm.Status()
		if err != nil {
			t.Logf("could not bring nodes back up after test: %v", err)
			return
		}
		for _, node := range ccmStatus {
			if node.State == ccm.NodeStateDown {
				err = ccm.NodeUp(node.Name)
				if err != nil {
					t.Logf("could not bring node %v back up after test: %v", node.Name, err)
				}
			}
		}
	}()

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
		t.Fatal(err)
	}

	testFilter.SetAllowedHosts(allAllowedHosts)

	if err = ccm.NodeUp(ccHostName); err != nil {
		t.Fatal(err)
	}

	assertNodeUp := func() error {
		hosts := session.ring.currentHosts()
		if len(hosts) != len(allCcmHosts) {
			return fmt.Errorf("expected %v hosts in ring but there were %v", len(allCcmHosts), len(hosts))
		}
		for _, host := range hosts {
			if !host.IsUp() {
				return fmt.Errorf("expected all hosts to be UP but %v isn't", host.String())
			}
		}
		session.pool.mu.RLock()
		poolsLen := len(session.pool.hostConnPools)
		session.pool.mu.RUnlock()
		if poolsLen != len(allCcmHosts) {
			return fmt.Errorf("expected %v connection pool but there were %v", len(allCcmHosts), poolsLen)
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
		t.Fatal(err)
	}
}
