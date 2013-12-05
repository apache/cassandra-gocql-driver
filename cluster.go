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

	"tux21b.org/v1/gocql/uuid"
)

// ClusterConfig is a struct to configure the default cluster implementation
// of gocoql. It has a varity of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that requre a
// different setup must implement their own cluster.
type ClusterConfig struct {
	Hosts        []string      // addresses for the initial connections
	CQLVersion   string        // CQL version (default: 3.0.0)
	ProtoVersion int           // version of the native protocol (default: 2)
	Timeout      time.Duration // connection timeout (default: 200ms)
	DefaultPort  int           // default port (default: 9042)
	Keyspace     string        // initial keyspace (optional)
	NumConns     int           // number of connections per host (default: 2)
	NumStreams   int           // number of streams per connection (default: 128)
	DelayMin     time.Duration // minimum reconnection delay (default: 1s)
	DelayMax     time.Duration // maximum reconnection delay (default: 10min)
	StartupMin   int           // wait for StartupMin hosts (default: len(Hosts)/2+1)
	Consistency  Consistency   // default consistency level (default: Quorum)
	Compressor   Compressor    // compression algorithm (default: nil)
	AutoDiscover bool          // To add nodes not supplied by the hosts array
	MaxConnRetry int
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:        hosts,
		CQLVersion:   "3.0.0",
		ProtoVersion: 2,
		Timeout:      200 * time.Millisecond,
		DefaultPort:  9042,
		NumConns:     2,
		NumStreams:   128,
		DelayMin:     1 * time.Second,
		DelayMax:     10 * time.Minute,
		StartupMin:   len(hosts)/2 + 1,
		Consistency:  Quorum,
		AutoDiscover: true,
		MaxConnRetry: 2,
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
		hostPool: NewHostPool(),
		quitWait: make(chan bool),
		keyspace: cfg.Keyspace,
	}
	impl.mu.Lock()
	hostConns := 0
	for i := 0; i < len(impl.cfg.Hosts); i++ {
		addr := strings.TrimSpace(impl.cfg.Hosts[i])
		if strings.Index(addr, ":") < 0 {
			addr = fmt.Sprintf("%s:%d", addr, impl.cfg.DefaultPort)
		}
		err := impl.connect(addr)
		if err == nil {
			hostConns++
			for j := 1; j < impl.cfg.NumConns; j++ {
				go impl.connect(addr)
			}
		}
	}
	//check if no host connections were made.
	if hostConns == 0 {
		return nil, ErrNoConns
	}
	impl.mu.Unlock()
	s := NewSession(impl)
	s.SetConsistency(cfg.Consistency)
	//TODO: Add code to begin autodiscovery queries.
	return s, nil
}

type clusterImpl struct {
	cfg      ClusterConfig
	hostPool *HostPool
	keyspace string
	mu       sync.Mutex

	started bool
	wgStart sync.WaitGroup

	quit     bool
	quitWait chan bool
	quitOnce sync.Once
}

func (c *clusterImpl) connect(addr string) error {
	cfg := ConnConfig{
		ProtoVersion: c.cfg.ProtoVersion,
		CQLVersion:   c.cfg.CQLVersion,
		Timeout:      c.cfg.Timeout,
		NumStreams:   c.cfg.NumStreams,
		Compressor:   c.cfg.Compressor,
	}
	delay := c.cfg.DelayMin
	retryCount := 0
	var conn *Conn
	var err error
	for retryCount < c.cfg.MaxConnRetry {
		conn, err = Connect(addr, cfg, c)
		if err != nil {
			log.Printf("failed to connect to %q: %v", addr, err)
			select {
			case <-time.After(delay):
				if delay *= 2; delay > c.cfg.DelayMax {
					delay = c.cfg.DelayMax
				}
				retryCount++
				continue
			case <-c.quitWait:
				return nil
			}
		} else {
			c.addConn(conn, "")
			return nil
		}
	}
	return err
}

func (c *clusterImpl) changeKeyspace(conn *Conn, keyspace string, connected bool) {
	if err := conn.UseKeyspace(keyspace); err != nil {
		conn.Close()
		if connected {
			c.hostPool.RemoveConn(conn)
		}
		go c.connect(conn.Address())
	}
	if !connected {
		c.addConn(conn, keyspace)
	}
}

func (c *clusterImpl) addConn(conn *Conn, keyspace string) {
	if c.quit {
		conn.Close()
		return
	}
	if keyspace != c.keyspace && c.keyspace != "" {
		// change the keyspace before adding the node to the pool
		c.changeKeyspace(conn, c.keyspace, false)
		return
	}
	//Fetch node information from the node itself.
	qryStr := "SELECT host_id,data_center,rack,bootstrapped FROM system.local"
	qry := &Query{stmt: qryStr, values: nil, cons: One, pageSize: 1, prefetch: 0.25}
	iter := conn.executeQuery(qry)
	if iter.err != nil {
		log.Printf("failed to get host information for %q, error %v", conn.Address, iter.err)
		conn.Close()
		return
	}
	var hostID uuid.UUID
	var dc, rack, ready string
	iter.Scan(&hostID, &dc, &rack, &ready)
	//check to make sure the host isn't bootstrapping
	if ready == "COMPLETED" {
		host := Host{
			HostID:     hostID,
			DataCenter: dc,
			Rack:       rack,
		}
		conn.hostID = host.HostID
		if pHost, ok := c.hostPool.pool[host.HostID]; ok {
			pHost.conn = append(pHost.conn, conn)
			host = pHost
		} else {
			host.conn = append(host.conn, conn)
			c.hostPool.AddHost(host)
		}
		//Check if there is room for connections
		if len(host.conn) < c.cfg.NumConns {
			defer c.connect(conn.Address())
		}
	} else {
		log.Printf("ignored host %q still bootstrapping.", conn.Address())
	}
	if err := iter.Close(); err != nil {
		log.Println(err)
	}
}

func (c *clusterImpl) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		// ignore all non-fatal errors
		return
	}
	c.hostPool.RemoveConn(conn)
	/*if !c.quit {
		go c.connect(conn.Address()) // reconnect
	}*/
	//reconnects will happen due to autodiscovery. This way the nodes can be validated against the cluster.
}

//HandleKeyspace is this still valid?
func (c *clusterImpl) HandleKeyspace(conn *Conn, keyspace string) {
	//Change code to loop through the pool and change keyspace.
	c.hostPool.mu.Lock()
	defer c.hostPool.mu.Unlock()
	if c.keyspace == keyspace {
		return
	}
	//Update all connections
	for _, host := range c.hostPool.pool {
		for conID := range host.conn {
			c.changeKeyspace(host.conn[conID], keyspace, true)
		}
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
		c.hostPool.Close()
	})
}

var (
	ErrNoHosts = errors.New("no hosts provided")
	ErrNoConns = errors.New("no host connections made")
)
