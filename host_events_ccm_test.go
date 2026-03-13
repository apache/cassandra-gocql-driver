//go:build ccm
// +build ccm

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

package gocql

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/ccm"
	"github.com/stretchr/testify/require"
)

type topologyChangeTestListener struct {
	hostAddedEvent   []NewHostEvent
	hostRemovedEvent []RemovedHostEvent
}

func (t *topologyChangeTestListener) OnNewHost(event NewHostEvent) {
	(*t).hostAddedEvent = append((*t).hostAddedEvent, event)
}

func (t *topologyChangeTestListener) OnRemovedHost(event RemovedHostEvent) {
	(*t).hostRemovedEvent = append((*t).hostRemovedEvent, event)
}

func TestTopologyChangesListener(t *testing.T) {
	err := ccm.StartAll()
	require.NoError(t, err)

	cluster, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)

	listener := &topologyChangeTestListener{}

	session := createSession(t, func(config *ClusterConfig) {
		config.Metadata.HostListener.TopologyChangeListener = listener
		config.Events.DisableTopologyEvents = false
		config.Hosts = cluster.HostAddrs()
	})
	defer session.Close()

	// adding a node should trigger a new node event

	newNodeName, newNodeIP, newNodeJMXPort := nextNodeSpec(t, cluster)

	t.Logf("Adding new node with name: %s, ip: %s, jmxPort: %d", newNodeName, newNodeIP, newNodeJMXPort)
	err = ccm.AddNode(newNodeName, newNodeIP, newNodeJMXPort)
	require.NoError(t, err)

	t.Logf("Starting node %s", newNodeName)
	err = ccm.NodeUp(newNodeName)
	require.NoError(t, err)

	// Expecting to see the new node event for the added node
	require.Eventually(t, func() bool {
		for _, event := range listener.hostAddedEvent {
			if event.Host.ConnectAddress().String() == newNodeIP {
				return true
			}
		}
		return false
	}, time.Second*60, time.Millisecond*200, "Expected new node event for %s", newNodeIP)

	t.Logf("Started node %s decommission", newNodeName)
	err = ccm.DecommissionNode(newNodeName)
	require.NoError(t, err)

	t.Logf("Removing node %s", newNodeName)
	err = ccm.RemoveNode(newNodeName)
	require.NoError(t, err)

	// Expecting to see the removed node event for the removed node
	require.Eventually(t, func() bool {
		for _, event := range listener.hostRemovedEvent {
			if event.Host.ConnectAddress().String() == newNodeIP {
				return true
			}
		}
		return false
	}, time.Second*120, time.Millisecond*200, "Expected removed node event for %s", newNodeIP)
}

// Determines the next node name, IP, and JMX port to use based on the existing nodes in the CCM cluster.
// Assumes a naming pattern of node{N} and IP pattern of 127.0.0.{N}.
func nextNodeSpec(t *testing.T, clusterMetadata *ccm.ClusterInfo) (name string, ip string, jmxPort int) {
	t.Helper()

	// Max node number for node{N} pattern
	maxNode := 0
	// The last octet of the highest IP address in the cluster, assuming a pattern of 127.0.0.{N}
	maxIPOctet := 0

	for _, host := range clusterMetadata.Hosts {
		// Pattern node{N}, so trying to find the max N to determine the next node name and IP
		if strings.HasPrefix(host.Name, "node") {
			if n, err := strconv.Atoi(strings.TrimPrefix(host.Name, "node")); err == nil && n > maxNode {
				maxNode = n
			}
		}

		ip := net.ParseIP(host.Addr)
		require.NotNil(t, ip, "Expected host address %q to be a valid IP", host.Addr)

		v4 := ip.To4()
		require.NotNil(t, v4, "Expected host address %q to be an IPv4 address", host.Addr)

		octet := int(v4[3])
		if octet > maxIPOctet {
			maxIPOctet = octet
		}
	}

	require.NotZero(t, maxNode, "Expected at least one node in CCM cluster")

	nextNodeNum := maxNode + 1
	ipLastOctet := nextNodeNum
	if maxIPOctet > 0 {
		ipLastOctet = maxIPOctet + 1
	}

	name = fmt.Sprintf("node%d", nextNodeNum)
	ip = fmt.Sprintf("127.0.0.%d", ipLastOctet)
	jmxPort = 7199 + (nextNodeNum - 1)

	return name, ip, jmxPort
}

type hostStateChangeTestListener struct {
	nodeUpEvent   []HostUpEvent
	nodeDownEvent []HostDownEvent
}

func (t *hostStateChangeTestListener) OnHostUp(event HostUpEvent) {
	(*t).nodeUpEvent = append((*t).nodeUpEvent, event)
}

func (t *hostStateChangeTestListener) OnHostDown(event HostDownEvent) {
	(*t).nodeDownEvent = append((*t).nodeDownEvent, event)
}

func (t *hostStateChangeTestListener) clear() {
	(*t).nodeUpEvent = nil
	(*t).nodeDownEvent = nil
}

func TestHostStateChangesListener(t *testing.T) {
	err := ccm.StartAll()
	require.NoError(t, err)

	cluster, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)

	listener := &hostStateChangeTestListener{}

	session := createSession(t, func(config *ClusterConfig) {
		config.Metadata.HostListener.HostStateChangeListener = listener
		config.Events.DisableNodeStatusEvents = false
		config.Hosts = cluster.HostAddrs()
		config.Logger = NewLogger(LogLevelDebug)
	})
	defer session.Close()

	listener.clear()

	// Stopping a node should trigger a node down event
	nodeToStop := cluster.Hosts[0]
	t.Logf("Stopping node %s", nodeToStop.Name)
	err = ccm.NodeDown(nodeToStop.Name)
	require.NoError(t, err)

	// Expecting to see the node down event for the stopped node
	require.Eventually(t, func() bool {
		for _, event := range listener.nodeDownEvent {
			if event.Host.ConnectAddress().String() == nodeToStop.Addr {
				return true
			}
		}
		return false
	}, time.Second*60, time.Millisecond*200, "Expected node down event for %s", nodeToStop.Addr)

	t.Logf("Starting node %s", nodeToStop.Name)
	err = ccm.NodeUp(nodeToStop.Name)
	require.NoError(t, err)

	// Expecting to see the node up event for the started node
	require.Eventually(t, func() bool {
		for _, event := range listener.nodeUpEvent {
			if event.Host.ConnectAddress().String() == nodeToStop.Addr {
				return true
			}
		}
		return false
	}, time.Second*60, time.Millisecond*200, "Expected node up event for %s", nodeToStop.Addr)
}

func TestHostListenersNeverCalledDuringSessionCreation(t *testing.T) {
	err := ccm.StartAll()
	require.NoError(t, err)

	cluster, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)

	hostStateChangeListener := &hostStateChangeTestListener{}
	topologyChangeListener := &topologyChangeTestListener{}

	session := createSession(t, func(config *ClusterConfig) {
		config.Metadata.HostListener.HostStateChangeListener = hostStateChangeListener
		config.Metadata.HostListener.TopologyChangeListener = topologyChangeListener
		config.Hosts = cluster.HostAddrs()
	})
	defer session.Close()

	require.Empty(t, hostStateChangeListener.nodeUpEvent)
	require.Empty(t, hostStateChangeListener.nodeDownEvent)
	require.Empty(t, topologyChangeListener.hostAddedEvent)
	require.Empty(t, topologyChangeListener.hostRemovedEvent)
}
