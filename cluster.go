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
	Consistency     Consistency   // default consistency level (default: Quorum)
	Compressor      Compressor    // compression algorithm (default: nil)
	Authenticator   Authenticator // authenticator (default: nil)
	RetryPolicy     RetryPolicy   // Default retry policy to use for queries (default: 0)
	SocketKeepalive time.Duration // The keepalive period to use, enabled if > 0 (default: 0)
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:        hosts,
		CQLVersion:   "3.0.0",
		ProtoVersion: 2,
		Timeout:      600 * time.Millisecond,
		DefaultPort:  9042,
		NumConns:     2,
		NumStreams:   128,
		Consistency:  Quorum,
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
		cfg:          *cfg,
		hostPool:     NewRoundRobin(),
		connPool:     make(map[string]*RoundRobin),
		conns:        make(map[*Conn]struct{}),
		quitWait:     make(chan bool),
		cFillingPool: make(chan int, 1),
		keyspace:     cfg.Keyspace,
	}
	//Walk through connecting to hosts. As soon as one host connects
	//defer the remaining connections to cluster.fillPool()
	for i := 0; i < len(impl.cfg.Hosts); i++ {
		addr := strings.TrimSpace(impl.cfg.Hosts[i])
		if strings.Index(addr, ":") < 0 {
			addr = fmt.Sprintf("%s:%d", addr, impl.cfg.DefaultPort)
		}
		err := impl.connect(addr)
		if err == nil {
			impl.cFillingPool <- 1
			go impl.fillPool()
			break
		}

	}
	//See if there are any connections in the pool
	impl.mu.Lock()
	conns := len(impl.conns)
	impl.mu.Unlock()
	if conns > 0 {
		s := NewSession(impl)
		s.SetConsistency(cfg.Consistency)
		return s, nil
	}
	impl.Close()
	return nil, ErrNoConnectionsStarted

}

type clusterImpl struct {
	cfg      ClusterConfig
	hostPool *RoundRobin
	connPool map[string]*RoundRobin
	conns    map[*Conn]struct{}
	keyspace string
	mu       sync.Mutex

	cFillingPool chan int

	quit     bool
	quitWait chan bool
	quitOnce sync.Once
}

func (c *clusterImpl) connect(addr string) error {
	cfg := ConnConfig{
		ProtoVersion:  c.cfg.ProtoVersion,
		CQLVersion:    c.cfg.CQLVersion,
		Timeout:       c.cfg.Timeout,
		NumStreams:    c.cfg.NumStreams,
		Compressor:    c.cfg.Compressor,
		Authenticator: c.cfg.Authenticator,
		Keepalive:     c.cfg.SocketKeepalive,
	}

	for {
		conn, err := Connect(addr, cfg, c)
		if err != nil {
			log.Printf("failed to connect to %q: %v", addr, err)
			return err
		}
		return c.addConn(conn)
	}
}

func (c *clusterImpl) addConn(conn *Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.quit {
		conn.Close()
		return nil
	}
	//Set the connection's keyspace if any before adding it to the pool
	if c.keyspace != "" {
		if err := conn.UseKeyspace(c.keyspace); err != nil {
			log.Printf("error setting connection keyspace. %v", err)
			conn.Close()
			return err
		}
	}
	connPool := c.connPool[conn.Address()]
	if connPool == nil {
		connPool = NewRoundRobin()
		c.connPool[conn.Address()] = connPool
		c.hostPool.AddNode(connPool)
	}
	connPool.AddNode(conn)
	c.conns[conn] = struct{}{}
	return nil
}

//fillPool manages the pool of connections making sure that each host has the correct
//amount of connections defined. Also the method will test a host with one connection
//instead of flooding the host with number of connections defined in the cluster config
func (c *clusterImpl) fillPool() {
	//Debounce large amounts of requests to fill pool
	select {
	case <-time.After(1 * time.Millisecond):
		return
	case <-c.cFillingPool:
		defer func() { c.cFillingPool <- 1 }()
	}

	c.mu.Lock()
	isClosed := c.quit
	c.mu.Unlock()
	//Exit if cluster(session) is closed
	if isClosed {
		return
	}
	//Walk through list of defined hosts
	for i := 0; i < len(c.cfg.Hosts); i++ {
		addr := strings.TrimSpace(c.cfg.Hosts[i])
		if strings.Index(addr, ":") < 0 {
			addr = fmt.Sprintf("%s:%d", addr, c.cfg.DefaultPort)
		}
		var numConns int = 1
		//See if the host already has connections in the pool
		c.mu.Lock()
		conns, ok := c.connPool[addr]
		c.mu.Unlock()

		if ok {
			//if the host has enough connections just exit
			numConns = conns.Size()
			if numConns >= c.cfg.NumConns {
				continue
			}
		} else {
			//See if the host is reachable
			if err := c.connect(addr); err != nil {
				continue
			}
		}
		//This is reached if the host is responsive and needs more connections
		//Create connections for host synchronously to mitigate flooding the host.
		go func(a string, conns int) {
			for ; conns < c.cfg.NumConns; conns++ {
				c.connect(addr)
			}
		}(addr, numConns)
	}
}

// Should only be called if c.mu is locked
func (c *clusterImpl) removeConnLocked(conn *Conn) {
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

func (c *clusterImpl) removeConn(conn *Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeConnLocked(conn)
}

func (c *clusterImpl) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		// ignore all non-fatal errors
		return
	}
	c.removeConn(conn)
	if !c.quit {
		go c.fillPool() // top off pool.
	}
}

func (c *clusterImpl) Pick(qry *Query) *Conn {
	//Check if connections are available
	c.mu.Lock()
	conns := len(c.conns)
	c.mu.Unlock()

	if conns == 0 {
		//try to populate the pool before returning.
		c.fillPool()
	}

	return c.hostPool.Pick(qry)
}

func (c *clusterImpl) Close() {
	c.quitOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.quit = true
		close(c.quitWait)
		for conn := range c.conns {
			c.removeConnLocked(conn)
		}
	})
}

var (
	ErrNoHosts       = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
)
