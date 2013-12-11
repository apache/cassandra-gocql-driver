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
	Hosts                []string          // addresses for the initial connections
	CQLVersion           string            // CQL version (default: 3.0.0)
	ProtoVersion         int               // version of the native protocol (default: 2)
	Timeout              time.Duration     // connection timeout (default: 200ms)
	DefaultPort          int               // default port (default: 9042)
	Keyspace             string            // initial keyspace (optional)
	NumConns             int               // number of connections per host (default: 2)
	NumStreams           int               // number of streams per connection (default: 128)
	DelayMin             time.Duration     // minimum reconnection delay (default: 1s)
	DelayMax             time.Duration     // maximum reconnection delay (default: 10min)
	StartupMin           int               // wait for StartupMin hosts (default: len(Hosts)/2+1)
	Consistency          Consistency       // default consistency level (default: Quorum)
	Compressor           Compressor        // compression algorithm (default: nil)
	AutoDiscoverInterval int               // The interval in seconds to execute auto discovery queries, 0 means to never auto discover.
	MaxConnRetry         int               // Maxium of attempts to retry a connection to a host before quiting
	PreferredDataCenter  string            // The datacenter name to execute queries against
	PreferredRack        string            // The rack name within the preferred dc to execute queries against
	DefaultRetryPolicy   RetryPolicy       // sets the default retry policy for queries.
	DefaultLBPolicy      LoadBalancePolicy // Sets the default load balancing policy to use for queries
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:                hosts,
		CQLVersion:           "3.0.0",
		ProtoVersion:         2,
		Timeout:              200 * time.Millisecond,
		DefaultPort:          9042,
		NumConns:             2,
		NumStreams:           128,
		DelayMin:             1 * time.Second,
		DelayMax:             10 * time.Minute,
		StartupMin:           len(hosts)/2 + 1,
		Consistency:          Quorum,
		AutoDiscoverInterval: 60,
		MaxConnRetry:         2,
		DefaultLBPolicy:      &RoundRobin{},
		DefaultRetryPolicy:   RetryPolicy{Host: 3, Rack: 1, DataCenter: 1},
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
		hostPool: NewHostPool(cfg.DefaultLBPolicy),
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
		//Make sure there was no error connecting to the host before making further connections
		if err == nil {
			hostConns++
			go func(address string, numConns int) {
				for j := 1; j < numConns; j++ {
					impl.connect(address)
				}
			}(addr, impl.cfg.NumConns)
		}
	}
	//check if no host connections were made.
	if hostConns == 0 {
		return nil, ErrNoConns
	}
	impl.mu.Unlock()
	s := NewSession(impl)
	s.SetConsistency(cfg.Consistency)
	//Kick off auto discovery
	go impl.autoDiscover()
	return s, nil
}

type clusterImpl struct {
	cfg              ClusterConfig
	hostPool         *HostPool
	keyspace         string
	mu               sync.Mutex
	lastAutoDiscover time.Time

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
			return c.addConn(conn, "")
		}
	}
	return err
}

func (c *clusterImpl) changeKeyspace(conn *Conn, keyspace string, connected bool) error {
	if err := conn.UseKeyspace(keyspace); err != nil {
		conn.Close()
		if connected {
			c.hostPool.RemoveConn(conn)
		}
		return c.connect(conn.Address())
	}
	if !connected {
		return c.addConn(conn, keyspace)
	}
	return nil
}

func (c *clusterImpl) addConn(conn *Conn, keyspace string) error {
	if c.quit {
		conn.Close()
		return nil
	}
	if keyspace != c.keyspace && c.keyspace != "" {
		// change the keyspace before adding the node to the pool
		return c.changeKeyspace(conn, c.keyspace, false)
	}
	//Fetch node information from the node itself.
	qryStr := "SELECT host_id,data_center,rack,bootstrapped FROM system.local"
	qry := &Query{stmt: qryStr, values: nil, cons: One}
	iter := conn.executeQuery(qry)
	if iter.err != nil {
		conn.Close()
		log.Printf("Iterator error: %v", iter.err)
		return iter.err
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
	} else {
		defer conn.Close()
		return ErrHostBs
	}
	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	return nil
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

//autoDiscover queries a host to pull cluster information and forms connections with available hosts
func (c *clusterImpl) autoDiscover() {
	if c.cfg.AutoDiscoverInterval < 1 || c.quit {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Now().Add(-time.Second * time.Duration(c.cfg.AutoDiscoverInterval)).After(c.lastAutoDiscover) {
		//Adjust last autodiscover time for after the execution
		c.lastAutoDiscover = time.Now()

		//Fetch node information from the node itself.
		qryStr := "SELECT peer,rpc_address,host_id FROM system.peers"
		qry := &Query{stmt: qryStr, values: nil, cons: One}
		conn := c.hostPool.Pick(nil)
		//Means no available connections in the pool. Reload pool by using seed list again
		if conn == nil {
			//TODO: Use seed list to restart cluster
		}
		itr := conn.executeQuery(qry)
		if itr.err != nil {
			log.Printf("failed to collect peer information %v", itr.err)
			return
		}
		var hid uuid.UUID
		var peerAddr, rpcAddr string
		//Get a read lock on the host pool and create connections with hosts.
		c.hostPool.mu.RLock()
		for itr.Scan(&peerAddr, &rpcAddr, &hid) {
			address := peerAddr
			numConns := 0
			if rpcAddr != "" || rpcAddr != "0.0.0.0" {
				address = rpcAddr
			}
			if host, exists := c.hostPool.pool[hid]; exists {
				numConns = len(host.conn)
			}
			go func(addr string, port int, nConns int, maxConn int) {
				for i := nConns; i < maxConn; i++ {
					c.connect(fmt.Sprintf("%s:%d", addr, port))
				}
			}(address, c.cfg.DefaultPort, numConns, c.cfg.NumConns)

		}
		c.hostPool.mu.RUnlock()
		//Schedule next autodiscover
		go func(i int) {
			time.Sleep(time.Second * time.Duration(i))
			c.autoDiscover()
		}(c.cfg.AutoDiscoverInterval)
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
	ErrHostBs  = errors.New("host is bootstrapping.")
)
