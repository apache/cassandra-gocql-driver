package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

/*ConnectionPool represents the interface gocql will use to work with a collection of connections.

Purpose

The connection pool in gocql opens and closes connections as well as selects an available connection
for gocql to execute a query against. The pool is also respnsible for handling connection errors that
are caught by the connection experiencing the error.

A connection pool should make a copy of the variables used from the ClusterConfig provided to the pool
upon creation. ClusterConfig is a pointer and can be modified after the creation of the pool. This can
lead to issues with variables being modified outside the expectations of the ConnectionPool type.

Example of Single Connection Pool:

	type SingleConnection struct {
		conn *Conn
		cfg *ClusterConfig
	}

	func NewSingleConnection(cfg *ClusterConfig) ConnectionPool {
		addr := JoinHostPort(cfg.Hosts[0], cfg.Port)

		connCfg := ConnConfig{
			ProtoVersion:  cfg.ProtoVersion,
			CQLVersion:    cfg.CQLVersion,
			Timeout:       cfg.Timeout,
			NumStreams:    cfg.NumStreams,
			Compressor:    cfg.Compressor,
			Authenticator: cfg.Authenticator,
			Keepalive:     cfg.SocketKeepalive,
		}
		pool := SingleConnection{cfg:cfg}
		pool.conn = Connect(addr,connCfg,pool)
		return &pool
	}

	func (s *SingleConnection) HandleError(conn *Conn, err error, closed bool) {
		if closed {
			connCfg := ConnConfig{
				ProtoVersion:  cfg.ProtoVersion,
				CQLVersion:    cfg.CQLVersion,
				Timeout:       cfg.Timeout,
				NumStreams:    cfg.NumStreams,
				Compressor:    cfg.Compressor,
				Authenticator: cfg.Authenticator,
				Keepalive:     cfg.SocketKeepalive,
			}
			s.conn = Connect(conn.Address(),connCfg,s)
		}
	}

	func (s *SingleConnection) Pick(qry *Query) *Conn {
		if s.conn.isClosed {
			return nil
		}
		return s.conn
	}

	func (s *SingleConnection) Size() int {
		return 1
	}

	func (s *SingleConnection) Close() {
		s.conn.Close()
	}

This is a very simple example of a type that exposes the connection pool interface. To assign
this type as the connection pool to use you would assign it to the ClusterConfig like so:

		cluster := NewCluster("127.0.0.1")
		cluster.ConnPoolType = NewSingleConnection
		...
		session, err := cluster.CreateSession()

To see a more complete example of a ConnectionPool implementation please see the SimplePool type.
*/
type ConnectionPool interface {
	Pick(*Query) *Conn
	Size() int
	HandleError(*Conn, error, bool)
	Close()
	SetHosts(host []HostInfo)
}

//NewPoolFunc is the type used by ClusterConfig to create a pool of a specific type.
type NewPoolFunc func(*ClusterConfig) (ConnectionPool, error)

//SimplePool is the current implementation of the connection pool inside gocql. This
//pool is meant to be a simple default used by gocql so users can get up and running
//quickly.
type SimplePool struct {
	cfg      *ClusterConfig
	hostPool *RoundRobin
	connPool map[string]*RoundRobin
	conns    map[*Conn]struct{}
	keyspace string

	hostMu sync.RWMutex
	// this is the set of current hosts which the pool will attempt to connect to
	hosts map[string]*HostInfo

	// protects hostpool, connPoll, conns, quit
	mu sync.Mutex

	cFillingPool chan int

	quit     bool
	quitWait chan bool
	quitOnce sync.Once

	tlsConfig *tls.Config
}

func setupTLSConfig(sslOpts *SslOptions) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	// ca cert is optional
	if sslOpts.CaPath != "" {
		pem, err := ioutil.ReadFile(sslOpts.CaPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to open CA certs: %v", err)
		}

		if !certPool.AppendCertsFromPEM(pem) {
			return nil, errors.New("connectionpool: failed parsing or CA certs")
		}
	}

	mycert, err := tls.LoadX509KeyPair(sslOpts.CertPath, sslOpts.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("connectionpool: unable to load X509 key pair: %v", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{mycert},
		RootCAs:      certPool,
	}

	config.InsecureSkipVerify = !sslOpts.EnableHostVerification

	return config, nil
}

//NewSimplePool is the function used by gocql to create the simple connection pool.
//This is the default if no other pool type is specified.
func NewSimplePool(cfg *ClusterConfig) (ConnectionPool, error) {
	pool := &SimplePool{
		cfg:          cfg,
		hostPool:     NewRoundRobin(),
		connPool:     make(map[string]*RoundRobin),
		conns:        make(map[*Conn]struct{}),
		quitWait:     make(chan bool),
		cFillingPool: make(chan int, 1),
		keyspace:     cfg.Keyspace,
		hosts:        make(map[string]*HostInfo),
	}

	for _, host := range cfg.Hosts {
		// seed hosts have unknown topology
		// TODO: Handle populating this during SetHosts
		pool.hosts[host] = &HostInfo{Peer: host}
	}

	if cfg.SslOpts != nil {
		config, err := setupTLSConfig(cfg.SslOpts)
		if err != nil {
			return nil, err
		}
		pool.tlsConfig = config
	}

	//Walk through connecting to hosts. As soon as one host connects
	//defer the remaining connections to cluster.fillPool()
	for i := 0; i < len(cfg.Hosts); i++ {
		addr := JoinHostPort(cfg.Hosts[i], cfg.Port)

		if pool.connect(addr) == nil {
			pool.cFillingPool <- 1
			go pool.fillPool()
			break
		}
	}

	return pool, nil
}

func (c *SimplePool) connect(addr string) error {

	cfg := ConnConfig{
		ProtoVersion:  c.cfg.ProtoVersion,
		CQLVersion:    c.cfg.CQLVersion,
		Timeout:       c.cfg.Timeout,
		NumStreams:    c.cfg.NumStreams,
		Compressor:    c.cfg.Compressor,
		Authenticator: c.cfg.Authenticator,
		Keepalive:     c.cfg.SocketKeepalive,
		tlsConfig:     c.tlsConfig,
	}

	conn, err := Connect(addr, cfg, c)
	if err != nil {
		log.Printf("connect: failed to connect to %q: %v", addr, err)
		return err
	}

	return c.addConn(conn)
}

func (c *SimplePool) addConn(conn *Conn) error {
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
func (c *SimplePool) fillPool() {
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

	c.hostMu.RLock()

	//Walk through list of defined hosts
	var wg sync.WaitGroup
	for host := range c.hosts {
		addr := JoinHostPort(host, c.cfg.Port)

		numConns := 1
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
		wg.Add(1)
		go func(a string, conns int) {
			defer wg.Done()
			for ; conns < c.cfg.NumConns; conns++ {
				c.connect(a)
			}
		}(addr, numConns)
	}

	c.hostMu.RUnlock()

	//Wait until we're finished connecting to each host before returning
	wg.Wait()
}

// Should only be called if c.mu is locked
func (c *SimplePool) removeConnLocked(conn *Conn) {
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

func (c *SimplePool) removeConn(conn *Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeConnLocked(conn)
}

//HandleError is called by a Connection object to report to the pool an error has occured.
//Logic is then executed within the pool to clean up the erroroneous connection and try to
//top off the pool.
func (c *SimplePool) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		// ignore all non-fatal errors
		return
	}
	c.removeConn(conn)
	c.mu.Lock()
	poolClosed := c.quit
	c.mu.Unlock()
	if !poolClosed {
		go c.fillPool() // top off pool.
	}
}

//Pick selects a connection to be used by the query.
func (c *SimplePool) Pick(qry *Query) *Conn {
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

//Size returns the number of connections currently active in the pool
func (p *SimplePool) Size() int {
	p.mu.Lock()
	conns := len(p.conns)
	p.mu.Unlock()
	return conns
}

//Close kills the pool and all associated connections.
func (c *SimplePool) Close() {
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

func (c *SimplePool) SetHosts(hosts []HostInfo) {

	c.hostMu.Lock()
	toRemove := make(map[string]struct{})
	for k := range c.hosts {
		toRemove[k] = struct{}{}
	}

	for _, host := range hosts {
		host := host

		delete(toRemove, host.Peer)
		// we already have it
		if _, ok := c.hosts[host.Peer]; ok {
			// TODO: Check rack, dc, token range is consistent, trigger topology change
			// update stored host
			continue
		}

		c.hosts[host.Peer] = &host
	}

	// can we hold c.mu whilst iterating this loop?
	for addr := range toRemove {
		c.removeHostLocked(addr)
	}
	c.hostMu.Unlock()

	c.fillPool()
}

func (c *SimplePool) removeHostLocked(addr string) {
	if _, ok := c.hosts[addr]; !ok {
		return
	}
	delete(c.hosts, addr)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.connPool[addr]; !ok {
		return
	}

	for conn := range c.conns {
		if conn.Address() == addr {
			c.removeConnLocked(conn)
		}
	}
}
