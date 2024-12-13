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
	"sync/atomic"
)

// Polls system.peers at a specific interval to find new hosts
type ringDescriber struct {
	control         *controlConn
	cfg             *ClusterConfig
	logger          StdLogger
	prevHosts       []*HostInfo
	prevPartitioner string

	// endpoints are the set of endpoints which the driver will attempt to connect
	// to in the case it can not reach any of its hosts. They are also used to boot
	// strap the initial connection.
	endpoints []*HostInfo

	mu sync.RWMutex
	// hosts are the set of all hosts in the cassandra ring that we know of.
	// key of map is host_id.
	hosts map[string]*HostInfo
	// hostIPToUUID maps host native address to host_id.
	hostIPToUUID map[string]string

	hostList []*HostInfo
	pos      uint32

	// TODO: we should store the ring metadata here also.
}

func (r *ringDescriber) setControlConn(c *controlConn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.control = c
}

// Ask the control node for the local host info
func (r *ringDescriber) getLocalHostInfo() (*HostInfo, error) {
	if r.control == nil {
		return nil, errNoControl
	}

	iter := r.control.withConnHost(func(ch *connHost) *Iter {
		return ch.conn.querySystemLocal(context.TODO())
	})

	if iter == nil {
		return nil, errNoControl
	}

	host, err := hostInfoFromIter(iter, nil, r.cfg.Port, r.cfg.translateAddressPort)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve local host info: %w", err)
	}
	return host, nil
}

// Ask the control node for host info on all it's known peers
func (r *ringDescriber) getClusterPeerInfo(localHost *HostInfo) ([]*HostInfo, error) {
	if r.control == nil {
		return nil, errNoControl
	}

	var peers []*HostInfo
	iter := r.control.withConnHost(func(ch *connHost) *Iter {
		return ch.conn.querySystemPeers(context.TODO(), localHost.version)
	})

	if iter == nil {
		return nil, errNoControl
	}

	rows, err := iter.SliceMap()
	if err != nil {
		// TODO(zariel): make typed error
		return nil, fmt.Errorf("unable to fetch peer host info: %s", err)
	}

	for _, row := range rows {
		// extract all available info about the peer
		host, err := hostInfoFromMap(row, &HostInfo{port: r.cfg.Port}, r.cfg.translateAddressPort)
		if err != nil {
			return nil, err
		} else if !isValidPeer(host) {
			// If it's not a valid peer
			r.logger.Printf("Found invalid peer '%s' "+
				"Likely due to a gossip or snitch issue, this host will be ignored", host)
			continue
		}

		peers = append(peers, host)
	}

	return peers, nil
}

// Return true if the host is a valid peer
func isValidPeer(host *HostInfo) bool {
	return !(len(host.RPCAddress()) == 0 ||
		host.hostId == "" ||
		host.dataCenter == "" ||
		host.rack == "" ||
		len(host.tokens) == 0)
}

// GetHosts returns a list of hosts found via queries to system.local and system.peers
func (r *ringDescriber) GetHosts() ([]*HostInfo, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	localHost, err := r.getLocalHostInfo()
	if err != nil {
		return r.prevHosts, r.prevPartitioner, err
	}

	peerHosts, err := r.getClusterPeerInfo(localHost)
	if err != nil {
		return r.prevHosts, r.prevPartitioner, err
	}

	hosts := append([]*HostInfo{localHost}, peerHosts...)
	var partitioner string
	if len(hosts) > 0 {
		partitioner = hosts[0].Partitioner()
	}

	return hosts, partitioner, nil
}

func (r *ringDescriber) rrHost() *HostInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hostList) == 0 {
		return nil
	}

	pos := int(atomic.AddUint32(&r.pos, 1) - 1)
	return r.hostList[pos%len(r.hostList)]
}

func (r *ringDescriber) getHostByIP(ip string) (*HostInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hi, ok := r.hostIPToUUID[ip]
	return r.hosts[hi], ok
}

func (r *ringDescriber) getHost(hostID string) *HostInfo {
	r.mu.RLock()
	host := r.hosts[hostID]
	r.mu.RUnlock()
	return host
}

func (r *ringDescriber) allHosts() []*HostInfo {
	r.mu.RLock()
	hosts := make([]*HostInfo, 0, len(r.hosts))
	for _, host := range r.hosts {
		hosts = append(hosts, host)
	}
	r.mu.RUnlock()
	return hosts
}

func (r *ringDescriber) currentHosts() map[string]*HostInfo {
	r.mu.RLock()
	hosts := make(map[string]*HostInfo, len(r.hosts))
	for k, v := range r.hosts {
		hosts[k] = v
	}
	r.mu.RUnlock()
	return hosts
}

func (r *ringDescriber) addOrUpdate(host *HostInfo) *HostInfo {
	if existingHost, ok := r.addHostIfMissing(host); ok {
		existingHost.update(host)
		host = existingHost
	}
	return host
}

func (r *ringDescriber) addHostIfMissing(host *HostInfo) (*HostInfo, bool) {
	if host.invalidConnectAddr() {
		panic(fmt.Sprintf("invalid host: %v", host))
	}
	hostID := host.HostID()

	r.mu.Lock()
	if r.hosts == nil {
		r.hosts = make(map[string]*HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	existing, ok := r.hosts[hostID]
	if !ok {
		r.hosts[hostID] = host
		r.hostIPToUUID[host.nodeToNodeAddress().String()] = hostID
		existing = host
		r.hostList = append(r.hostList, host)
	}
	r.mu.Unlock()
	return existing, ok
}

func (r *ringDescriber) removeHost(hostID string) bool {
	r.mu.Lock()
	if r.hosts == nil {
		r.hosts = make(map[string]*HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	h, ok := r.hosts[hostID]
	if ok {
		for i, host := range r.hostList {
			if host.HostID() == hostID {
				r.hostList = append(r.hostList[:i], r.hostList[i+1:]...)
				break
			}
		}
		delete(r.hostIPToUUID, h.nodeToNodeAddress().String())
	}
	delete(r.hosts, hostID)
	r.mu.Unlock()
	return ok
}

type clusterMetadata struct {
	mu          sync.RWMutex
	partitioner string
}

func (c *clusterMetadata) setPartitioner(partitioner string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.partitioner != partitioner {
		// TODO: update other things now
		c.partitioner = partitioner
	}
}
