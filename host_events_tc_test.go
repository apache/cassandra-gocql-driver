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

package gocql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type topologyChangeTestListener struct {
	hostAddedEvent   []NewHostEvent
	hostRemovedEvent []RemovedHostEvent
}

func (t *topologyChangeTestListener) OnNewHost(event NewHostEvent) {
	t.hostAddedEvent = append(t.hostAddedEvent, event)
}

func (t *topologyChangeTestListener) OnRemovedHost(event RemovedHostEvent) {
	t.hostRemovedEvent = append(t.hostRemovedEvent, event)
}

type hostStateChangeTestListener struct {
	nodeUpEvent   []HostUpEvent
	nodeDownEvent []HostDownEvent
}

func (t *hostStateChangeTestListener) OnHostUp(event HostUpEvent) {
	t.nodeUpEvent = append(t.nodeUpEvent, event)
}

func (t *hostStateChangeTestListener) OnHostDown(event HostDownEvent) {
	t.nodeDownEvent = append(t.nodeDownEvent, event)
}

func (t *hostStateChangeTestListener) clear() {
	t.nodeUpEvent = nil
	t.nodeDownEvent = nil
}

func TestHostStateChangesListener(t *testing.T) {
	ctx := context.Background()
	listener := &hostStateChangeTestListener{}

	session := createSession(t, func(config *ClusterConfig) {
		config.Metadata.HostListener.HostStateChangeListener = listener
		config.Events.DisableNodeStatusEvents = false
		config.Logger = NewLogger(LogLevelDebug)
	})
	defer session.Close()

	listener.clear()

	nodeToStop := cassNodes["node1"]
	t.Logf("Stopping node %s", "node1")
	require.NoError(t, nodeToStop.TC.Stop(ctx, nil))
	t.Cleanup(func() {
		_ = restoreCluster(ctx)
	})

	require.Eventually(t, func() bool {
		for _, event := range listener.nodeDownEvent {
			if event.Host.ConnectAddress().String() == nodeToStop.Addr {
				return true
			}
		}
		return false
	}, time.Minute, 200*time.Millisecond, "expected node down event for %s", nodeToStop.Addr)

	require.NoError(t, restoreCluster(ctx))

	require.Eventually(t, func() bool {
		for _, event := range listener.nodeUpEvent {
			if event.Host.ConnectAddress().String() == nodeToStop.Addr {
				return true
			}
		}
		return false
	}, time.Minute, 200*time.Millisecond, "expected node up event for %s", nodeToStop.Addr)
}

func TestHostListenersNeverCalledDuringSessionCreation(t *testing.T) {
	hostStateChangeListener := &hostStateChangeTestListener{}
	topologyChangeListener := &topologyChangeTestListener{}

	session := createSession(t, func(config *ClusterConfig) {
		config.Metadata.HostListener.HostStateChangeListener = hostStateChangeListener
		config.Metadata.HostListener.TopologyChangeListener = topologyChangeListener
	})
	defer session.Close()

	require.Empty(t, hostStateChangeListener.nodeUpEvent)
	require.Empty(t, hostStateChangeListener.nodeDownEvent)
	require.Empty(t, topologyChangeListener.hostAddedEvent)
	require.Empty(t, topologyChangeListener.hostRemovedEvent)
}
