// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"sync"
)

//Host represents the structure for storing key host information
//that is used by load balancing and fail over policies.
type Host struct {
	HostID     UUID
	DataCenter string
	Rack       string
	conn       []*Conn //Array of connections to host
}

type HostPool struct {
	pool        map[UUID]Host //A map of hosts
	topologyMap map[string]map[string][]UUID
	hostIDs     []UUID
	mu          sync.RWMutex
	defaultLBP  LoadBalancePolicy
}

//NewHostPool creates a new host pool
func NewHostPool(lbp LoadBalancePolicy) *HostPool {
	return &HostPool{
		pool:        make(map[UUID]Host),
		topologyMap: make(map[string]map[string][]UUID),
		defaultLBP:  lbp,
	}
}

//AddHost adds the host to the pool and updates the topology
//map so that it is available for queries.
func (p *HostPool) AddHost(host Host) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.pool[host.HostID]; exists {
		return
	}

	p.pool[host.HostID] = host
	p.hostIDs = append(p.hostIDs, host.HostID)

	var (
		dc map[string][]UUID
		ok bool
	)

	//Check if the map for the datacenter exists
	//If not create one.
	if dc, ok = p.topologyMap[host.DataCenter]; !ok {
		dc = make(map[string][]UUID)
		p.topologyMap[host.DataCenter] = dc
	}

	//Update the array of host ids for the rack.
	dc[host.Rack] = append(dc[host.Rack], host.HostID)
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

	//Remove host from hostIDs list
	n = len(p.hostIDs)
	for i := 0; i < n; i++ {
		p.hostIDs[i], p.hostIDs[n-1] = p.hostIDs[n-1], p.hostIDs[i]
		p.hostIDs = p.hostIDs[:n-1]
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
// Zariel: Can we move the picking logic down into another interface?
func (p *HostPool) Pick(qry *Query) *Conn {
	// Can we make this lock more finely grained?
	p.mu.RLock()
	defer p.mu.RUnlock()

	if qry == nil {
		return p.defaultLBP.Pick(p.hostIDs, p.pool)
	}

	rtp := qry.rtPolicy
	//Check if a query is being retried
	if rtp.Count > 0 {
		h := p.pool[qry.lastHostID]

		if rtp.Count < rtp.Rack {
			//Retrying the query in the same rack
			hList := p.topologyMap[h.DataCenter][h.Rack]

			if len(hList) > 1 {
				return qry.lbPolicy.Pick(hList, p.pool)
			}
		} else if rtp.Count >= rtp.Rack && rtp.DataCenter > 0 {
			//Retrying the query in a different datacenter
			if len(p.topologyMap) > 1 {
				//Select the next datacenter
				for n, dc := range p.topologyMap {
					//Only if the datacenter does not match the last host
					if n != h.DataCenter {
						//Select a rack inside the datacenter
						for _, r := range dc {
							return qry.lbPolicy.Pick(r, p.pool)
						}
					}
				}
			}
		}
	} else if (qry.prefDC != "" && qry.prefRack != "") || qry.prefDC != "" {
		//topology preferences have been defined
		dc := p.topologyMap[qry.prefDC]

		if dc != nil {
			rack := dc[qry.prefRack]

			if rack == nil {
				//Get a rack that is defined in the DC
				for _, rack = range dc {
					break
				}
			}

			return qry.lbPolicy.Pick(rack, p.pool)
		}
	}

	return qry.lbPolicy.Pick(p.hostIDs, p.pool)
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

type Node interface {
	Pick(qry *Query) *Conn
	Close()
}
