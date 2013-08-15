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
type ClusterConfig struct {
	Hosts        []string
	CQLVersion   string
	ProtoVersion int
	Timeout      time.Duration
	DefaultPort  int
	Keyspace     string
	NumConn      int
	NumStreams   int
	DelayMin     time.Duration
	DelayMax     time.Duration
	StartupMin   int
}

func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:        hosts,
		CQLVersion:   "3.0.0",
		ProtoVersion: 2,
		Timeout:      200 * time.Millisecond,
		DefaultPort:  9042,
		NumConn:      2,
		DelayMin:     1 * time.Second,
		DelayMax:     10 * time.Minute,
		StartupMin:   len(hosts)/2 + 1,
	}
	return cfg
}

func (cfg *ClusterConfig) CreateSession() *Session {
	impl := &clusterImpl{
		cfg:      *cfg,
		hostPool: NewRoundRobin(),
		connPool: make(map[string]*RoundRobin),
	}
	impl.wgStart.Add(1)
	impl.startup()
	impl.wgStart.Wait()
	return NewSession(impl)
}

type clusterImpl struct {
	cfg      ClusterConfig
	hostPool *RoundRobin
	connPool map[string]*RoundRobin
	mu       sync.RWMutex

	conns []*Conn

	started bool
	wgStart sync.WaitGroup

	quit     bool
	quitWait chan bool
	quitOnce sync.Once

	keyspace string
}

func (c *clusterImpl) startup() {
	for i := 0; i < len(c.cfg.Hosts); i++ {
		addr := strings.TrimSpace(c.cfg.Hosts[i])
		if strings.IndexByte(addr, ':') < 0 {
			addr = fmt.Sprintf("%s:%d", addr, c.cfg.DefaultPort)
		}
		for j := 0; j < c.cfg.NumConn; j++ {
			go c.connect(addr)
		}
	}
}

func (c *clusterImpl) connect(addr string) {
	cfg := ConnConfig{
		ProtoVersion: 2,
		CQLVersion:   c.cfg.CQLVersion,
		Timeout:      c.cfg.Timeout,
		NumStreams:   c.cfg.NumStreams,
	}
	delay := c.cfg.DelayMin
	for {
		conn, err := Connect(addr, cfg, c)
		if err != nil {
			select {
			case <-time.After(delay):
				if delay *= 2; delay > c.cfg.DelayMax {
					delay = c.cfg.DelayMax
				}
				continue
			case <-c.quitWait:
				return
			}
		}
		c.addConn(conn, "")
		return
	}
}

func (c *clusterImpl) changeKeyspace(conn *Conn, keyspace string, connected bool) {
	if err := conn.UseKeyspace(keyspace); err != nil {
		conn.Close()
		if connected {
			c.removeConn(conn)
		}
		go c.connect(conn.Address())
	}
	if !connected {
		c.addConn(conn, keyspace)
	}
}

func (c *clusterImpl) addConn(conn *Conn, keyspace string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.quit {
		conn.Close()
		return
	}
	if keyspace != c.keyspace && c.keyspace != "" {
		go c.changeKeyspace(conn, c.keyspace, false)
		return
	}
	connPool := c.connPool[conn.Address()]
	if connPool == nil {
		connPool = NewRoundRobin()
		c.connPool[conn.Address()] = connPool
		c.hostPool.AddNode(connPool)
		if !c.started && c.hostPool.Size() >= c.cfg.StartupMin {
			c.started = true
			c.wgStart.Done()
		}
	}
	connPool.AddNode(conn)
	c.conns = append(c.conns, conn)
}

func (c *clusterImpl) removeConn(conn *Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn.Close()
	connPool := c.connPool[conn.addr]
	if connPool == nil {
		return
	}
	connPool.RemoveNode(conn)
	if connPool.Size() == 0 {
		c.hostPool.RemoveNode(connPool)
	}
	for i := 0; i < len(c.conns); i++ {
		if c.conns[i] == conn {
			last := len(c.conns) - 1
			c.conns[i], c.conns[last] = c.conns[last], c.conns[i]
			c.conns = c.conns[:last]
		}
	}
}

func (c *clusterImpl) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		return
	}
	c.removeConn(conn)
	go c.connect(conn.Address())
}

func (c *clusterImpl) HandleKeyspace(conn *Conn, keyspace string) {
	c.mu.Lock()
	if c.keyspace == keyspace {
		c.mu.Unlock()
		return
	}
	c.keyspace = keyspace
	conns := make([]*Conn, len(c.conns))
	copy(conns, c.conns)
	c.mu.Unlock()

	for i := 0; i < len(conns); i++ {
		if conns[i] == conn {
			continue
		}
		c.changeKeyspace(conns[i], keyspace, true)
	}
}

func (c *clusterImpl) ExecuteQuery(qry *Query) (*Iter, error) {
	return c.hostPool.ExecuteQuery(qry)
}

func (c *clusterImpl) ExecuteBatch(batch *Batch) error {
	return c.hostPool.ExecuteBatch(batch)
}

func (c *clusterImpl) Close() {
	c.quitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.quit = true
		close(c.quitWait)
		for i := 0; i < len(c.conns); i++ {
			c.conns[i].Close()
		}
	})
}
