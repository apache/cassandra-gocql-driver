// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Cluster sets up and maintains the node configuration of a Cassandra
// cluster.
//
// It has a varity of attributes that can be used to modify the behavior
// to fit the most common use cases. Applications that requre a different
// a setup should compose the nodes on their own.
type Cluster struct {
	Hosts       []string
	CQLVersion  string
	Timeout     time.Duration
	DefaultPort int
	Keyspace    string
	ConnPerHost int
	DelayMin    time.Duration
	DelayMax    time.Duration
}

func NewCluster(hosts ...string) *Cluster {
	c := &Cluster{
		Hosts:       hosts,
		CQLVersion:  "3.0.0",
		Timeout:     200 * time.Millisecond,
		DefaultPort: 9042,
		ConnPerHost: 2,
	}
	return c
}

func (c *Cluster) CreateSession() *Session {
	return NewSession(newClusterNode(c))
}

type clusterNode struct {
	cfg      Cluster
	hostPool *RoundRobin
	connPool map[string]*RoundRobin
	closed   bool
	mu       sync.Mutex
}

func newClusterNode(cfg *Cluster) *clusterNode {
	c := &clusterNode{
		cfg:      *cfg,
		hostPool: NewRoundRobin(),
		connPool: make(map[string]*RoundRobin),
	}
	for i := 0; i < len(c.cfg.Hosts); i++ {
		addr := strings.TrimSpace(c.cfg.Hosts[i])
		if strings.IndexByte(addr, ':') < 0 {
			addr = fmt.Sprintf("%s:%d", addr, c.cfg.DefaultPort)
		}
		for j := 0; j < c.cfg.ConnPerHost; j++ {
			go c.connect(addr)
		}
	}
	<-time.After(c.cfg.Timeout)
	return c
}

func (c *clusterNode) connect(addr string) {
	delay := c.cfg.DelayMin
	for {
		conn, err := Connect(addr, c.cfg.CQLVersion, c.cfg.Timeout)
		if err != nil {
			fmt.Println(err)
			<-time.After(delay)
			if delay *= 2; delay > c.cfg.DelayMax {
				delay = c.cfg.DelayMax
			}
			continue
		}
		c.addConn(addr, conn)
		return
	}
}

func (c *clusterNode) addConn(addr string, conn *Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	connPool := c.connPool[addr]
	if connPool == nil {
		connPool = NewRoundRobin()
		c.connPool[addr] = connPool
		c.hostPool.AddNode(connPool)
	}
	connPool.AddNode(conn)
	go func() {
		conn.Serve()
		c.removeConn(addr, conn)
	}()
}

func (c *clusterNode) removeConn(addr string, conn *Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pool := c.connPool[addr]
	if pool == nil {
		return
	}
	pool.RemoveNode(conn)
}

func (c *clusterNode) ExecuteQuery(qry *Query) (*Iter, error) {
	return c.hostPool.ExecuteQuery(qry)
}

func (c *clusterNode) ExecuteBatch(batch *Batch) error {
	return c.hostPool.ExecuteBatch(batch)
}

func (c *clusterNode) Close() {
	c.hostPool.Close()
}
