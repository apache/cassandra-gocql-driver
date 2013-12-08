// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"sync"
	"sync/atomic"
	"tux21b.org/v1/gocql/uuid"
)

/*********** New Topology Code *******/
//Host represents the structure for storing key host information
//that is used by load balancing and fail over policies.
type Host struct {
	HostID     uuid.UUID
	DataCenter string
	Rack       string
	conn       []*Conn //Array of connections to host
}

type HostPool struct {
	pool        map[uuid.UUID]Host //A map of hosts
	topologyMap map[string]map[string][]uuid.UUID
	mu          sync.RWMutex
}

//NewHostPool creates a new host pool
func NewHostPool() *HostPool {
	return &HostPool{
		pool:        make(map[uuid.UUID]Host),
		topologyMap: make(map[string]map[string][]uuid.UUID),
	}
}

//AddHost adds the host to the pool and updates the topology
//map so that it is available for queries.
func (p *HostPool) AddHost(host Host) {
	p.mu.Lock()
	p.pool[host.HostID] = host

	var dc map[string][]uuid.UUID
	var ok bool
	//Check if the map for the datacenter exists
	//If not create one.
	if dc, ok = p.topologyMap[host.DataCenter]; !ok {
		dc = make(map[string][]uuid.UUID)
		p.topologyMap[host.DataCenter] = dc
	}
	//Update the array of host ids for the rack.
	dc[host.Rack] = append(dc[host.Rack], host.HostID)
	p.mu.Unlock()
}

//RemoveHost removes the host from the topologymap, closes open connections
//and finally removes the host from the available pool
func (p *HostPool) RemoveHost(host Host) {
	p.mu.Lock()
	defer p.mu.Unlock()

	//Remove host from topologyMap
	dc := p.topologyMap[host.DataCenter]
	rack := dc[host.Rack]
	n := len(rack)
	if n == 1 {
		delete(dc, host.Rack)
		if len(dc) == 0 {
			delete(p.topologyMap, host.DataCenter)
		}
	} else {
		for i := 0; i < n; i++ {
			if rack[i] == host.HostID {
				rack[i], rack[n-1] = rack[n-1], rack[i]
				dc[host.Rack] = rack[:n-1]
				break
			}
		}
	}
	//Remove Host from pool
	delete(p.pool, host.HostID)

	//Close down all the connections for the host
	for i := 0; i < len(host.conn); i++ {
		host.conn[i].Close()
	}
}

//RemoveConn will remove the connection from the host.
//If the host no longer has anymore valid connections
//it will be removed from the pool.
func (p *HostPool) RemoveConn(conn *Conn) {
	p.mu.Lock()
	host := p.pool[conn.hostID]
	conns := len(host.conn)
	if conns > 1 {
		for i := 0; i < conns; i++ {
			if host.conn[i] == conn {
				host.conn[i], host.conn[conns-1] = host.conn[conns-1], host.conn[i]
				host.conn = host.conn[:conns-1]
				conn.Close()
				break
			}
		}
	} else {
		//Remove the host as it may be unhealthy. Let auto discover reconnect.
		defer p.RemoveHost(host)
	}
	p.mu.Unlock()
}

//Size returns the total size of the pool
func (r *HostPool) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.pool)
}

//Pick selects a host randomly that has atleast one stream available
//for processing the provided query.
//TODO: Add code to use the cluster defined preferred Datacenter and or rack.
func (r *HostPool) Pick(qry *Query) *Conn {
	r.mu.RLock()
	defer r.mu.RUnlock()

	//Use range on map to select a host.
	for _, val := range r.pool {
		conns := len(val.conn)
		//Check the host has open connections
		if conns > 0 {
			//Walk through the list of connections to get one
			//with an available stream
			for i := 0; i < conns; i++ {
				if len(val.conn[i].uniq) > 0 {
					return val.conn[i]
				}
			}
		} else {
			//Remove the host as it no longer has any valid connections
			//Auto discover will reconnect if the host is valid.
			go r.RemoveHost(val)
		}

	}
	return nil
}

//Close tears down the pool and closes all the open connections,
//the pool is no longer usable afterwards and should be discarded.
func (r *HostPool) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	//Nil the topologymap
	r.topologyMap = nil
	//Close all the connections
	for _, host := range r.pool {
		conns := len(host.conn)
		for i := 0; i < conns; i++ {
			//Maybe add code to wait for streams to complete with a timeout?
			host.conn[i].Close()
		}
	}
	r.pool = nil
}

/*********** End New Topology Code *******/

type Node interface {
	Pick(qry *Query) *Conn
	Close()
}

type RoundRobin struct {
	pool []Node
	pos  uint32
	mu   sync.RWMutex
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (r *RoundRobin) AddNode(node Node) {
	r.mu.Lock()
	r.pool = append(r.pool, node)
	r.mu.Unlock()
}

func (r *RoundRobin) RemoveNode(node Node) {
	r.mu.Lock()
	n := len(r.pool)
	for i := 0; i < n; i++ {
		if r.pool[i] == node {
			r.pool[i], r.pool[n-1] = r.pool[n-1], r.pool[i]
			r.pool = r.pool[:n-1]
			break
		}
	}
	r.mu.Unlock()
}

func (r *RoundRobin) Size() int {
	r.mu.RLock()
	n := len(r.pool)
	r.mu.RUnlock()
	return n
}

func (r *RoundRobin) Pick(qry *Query) *Conn {
	pos := atomic.AddUint32(&r.pos, 1)
	var node Node
	r.mu.RLock()
	if len(r.pool) > 0 {
		node = r.pool[pos%uint32(len(r.pool))]
	}
	r.mu.RUnlock()
	if node == nil {
		return nil
	}
	return node.Pick(qry)
}

func (r *RoundRobin) Close() {
	r.mu.Lock()
	for i := 0; i < len(r.pool); i++ {
		r.pool[i].Close()
	}
	r.pool = nil
	r.mu.Unlock()
}
