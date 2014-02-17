// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ClusterConfig is a struct to configure the default cluster implementation
// of gocoql. It has a varity of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that requre a
// different setup must implement their own cluster.
type ClusterConfig struct {
	Hosts           []string      // addresses for the initial connections
	CQLVersion      string        // CQL version (default: 3.0.0)
	ProtoVersion    int           // version of the native protocol (default: 2)
	Timeout         time.Duration // connection timeout (default: 600ms)
	DefaultPort     int           // default port (default: 9042)
	Keyspace        string        // initial keyspace (optional)
	NumConns        int           // number of connections per host (default: 2)
	NumStreams      int           // number of streams per connection (default: 128)
	DelayMin        time.Duration // minimum reconnection delay (default: 1s)
	DelayMax        time.Duration // maximum reconnection delay (default: 10min)
	StartupMin      int           // wait for StartupMin hosts (default: len(Hosts)/2+1)
	StartupTimeout  time.Duration // amount of to wait for a connection (default: 5s)
	Consistency     Consistency   // default consistency level (default: Quorum)
	Compressor      Compressor    // compression algorithm (default: nil)
	Authenticator   Authenticator // authenticator (default: nil)
	RetryPolicy     RetryPolicy   // Default retry policy to use for queries (default: 0)
	SocketKeepalive time.Duration // The keepalive period to use, enabled if > 0 (default: 0)
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:          hosts,
		CQLVersion:     "3.0.0",
		ProtoVersion:   2,
		Timeout:        600 * time.Millisecond,
		DefaultPort:    9042,
		NumConns:       2,
		NumStreams:     128,
		DelayMin:       1 * time.Second,
		DelayMax:       10 * time.Minute,
		StartupMin:     len(hosts)/2 + 1,
		StartupTimeout: 5 * time.Second,
		Consistency:    Quorum,
	}
	return cfg
}

// CreateSession initializes the cluster based on this config and returns a
// session object that can be used to interact with the database.
func (cfg *ClusterConfig) CreateSession() (*Session, error) {

	//Check that hosts in the ClusterConfig is not empty
	if len(cfg.Hosts) < 1 {
		return nil, ErrNoHosts
	}

	impl := &clusterImpl{
		cfg:      *cfg,
		hostPool: NewRoundRobin(),
		connPool: make(map[string]*RoundRobin),
		conns:    make(map[*Conn]struct{}),
		quitWait: make(chan bool),
		cStart:   make(chan int, 1),
		keyspace: cfg.Keyspace,
	}
	for i := 0; i < len(impl.cfg.Hosts); i++ {
		addr := strings.TrimSpace(impl.cfg.Hosts[i])
		if strings.Index(addr, ":") < 0 {
			addr = fmt.Sprintf("%s:%d", addr, impl.cfg.DefaultPort)
		}
		for j := 0; j < impl.cfg.NumConns; j++ {
			go impl.connect(addr)
		}
	}
	//See if a connection is made within the StartupTimeout window.
	select {
	case <-impl.cStart:
		s := NewSession(impl)
		s.SetConsistency(cfg.Consistency)
		return s, nil
	case <-time.After(cfg.StartupTimeout):
		impl.Close()
		return nil, ErrNoConnections
	}

}

type clusterImpl struct {
	cfg      ClusterConfig
	hostPool *RoundRobin
	connPool map[string]*RoundRobin
	conns    map[*Conn]struct{}
	keyspace string
	mu       sync.Mutex

	started bool
	cStart  chan int

	quit     bool
	quitWait chan bool
	quitOnce sync.Once
}

func (c *clusterImpl) connect(addr string) {
	cfg := ConnConfig{
		ProtoVersion:  c.cfg.ProtoVersion,
		CQLVersion:    c.cfg.CQLVersion,
		Timeout:       c.cfg.Timeout,
		NumStreams:    c.cfg.NumStreams,
		Compressor:    c.cfg.Compressor,
		Authenticator: c.cfg.Authenticator,
		Keepalive:     c.cfg.SocketKeepalive,
	}

	delay := c.cfg.DelayMin
	for {
		conn, err := Connect(addr, cfg, c)
		if err != nil {
			log.Printf("failed to connect to %q: %v", addr, err)
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
		// change the keyspace before adding the node to the pool
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
			c.cStart <- 1
		}
	}
	connPool.AddNode(conn)
	c.conns[conn] = struct{}{}
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
		delete(c.connPool, conn.addr)
	}
	delete(c.conns, conn)
}

func (c *clusterImpl) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		// ignore all non-fatal errors
		return
	}
	c.removeConn(conn)
	if !c.quit {
		go c.connect(conn.Address()) // reconnect
	}
}

func (c *clusterImpl) HandleKeyspace(conn *Conn, keyspace string) {
	c.mu.Lock()
	if c.keyspace == keyspace {
		c.mu.Unlock()
		return
	}
	c.keyspace = keyspace
	conns := make([]*Conn, 0, len(c.conns))
	for conn := range c.conns {
		conns = append(conns, conn)
	}
	c.mu.Unlock()

	// change the keyspace of all other connections too
	for i := 0; i < len(conns); i++ {
		if conns[i] == conn {
			continue
		}
		c.changeKeyspace(conns[i], keyspace, true)
	}
}

func (c *clusterImpl) Pick(qry *Query) *Conn {
	return c.hostPool.Pick(qry)
}

func (c *clusterImpl) Close() {
	c.quitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.quit = true
		close(c.quitWait)
		for conn := range c.conns {
			conn.Close()
		}
	})
}

var (
	ErrNoHosts       = errors.New("no hosts provided")
	ErrNoConnections = errors.New("no connections were made in startup time frame")
)
