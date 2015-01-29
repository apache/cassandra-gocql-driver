package gocql

import (
	"log"
	"net"
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
	Close()
	SetHosts(hosts []*HostInfo, partitioner string)
}

//NewPoolFunc is the type used by ClusterConfig to create a pool of a specific type.
type NewPoolFunc func(*ClusterConfig) ConnectionPool

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
}

//NewSimplePool is the function used by gocql to create the simple connection pool.
//This is the default if no other pool type is specified.
func NewSimplePool(cfg *ClusterConfig) ConnectionPool {
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

	return pool
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
		SslOpts:       c.cfg.SslOpts,
	}

	conn, err := Connect(addr, cfg, c.HandleError)
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
		go func(a string, conns int) {
			for ; conns < c.cfg.NumConns; conns++ {
				c.connect(a)
			}
		}(addr, numConns)
	}

	c.hostMu.RUnlock()
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
	if !c.quit {
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

func (c *SimplePool) SetHosts(hosts []*HostInfo, partitioner string) {

	c.hostMu.Lock()
	toRemove := make(map[string]struct{})
	for k := range c.hosts {
		toRemove[k] = struct{}{}
	}

	for _, host := range hosts {
		delete(toRemove, host.Peer)
		// we already have it
		if _, ok := c.hosts[host.Peer]; ok {
			// TODO: Check rack, dc, token range is consistent, trigger topology change
			// update stored host
			continue
		}

		c.hosts[host.Peer] = host
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

//NewRoundRobinConnPool creates a connection pool which selects hosts by
//round-robin, and then selects a connection for that host by round-robin.
func NewRoundRobinConnPool(cfg *ClusterConfig) ConnectionPool {
	return newPolicyConnPool(
		cfg,
		NewRoundRobinHostPolicy(),
		NewRoundRobinConnPolicy,
	)
}

//NewTokenAwareConnPool creates a connection pool which selects hosts by
//a token aware policy, and then selects a connection for that host by
//round-robin.
func NewTokenAwareConnPool(cfg *ClusterConfig) ConnectionPool {
	return newPolicyConnPool(
		cfg,
		NewTokenAwareHostPolicy(NewRoundRobinHostPolicy()),
		NewRoundRobinConnPolicy,
	)
}

type policyConnPool struct {
	port     int
	numConns int
	connCfg  ConnConfig
	keyspace string

	mu            sync.RWMutex
	hostPolicy    HostSelectionPolicy
	connPolicy    func() ConnSelectionPolicy
	hostConnPools map[string]*hostConnPool
}

func newPolicyConnPool(
	cfg *ClusterConfig,
	hostPolicy HostSelectionPolicy,
	connPolicy func() ConnSelectionPolicy,
) ConnectionPool {
	// create the pool
	pool := &policyConnPool{
		port:     cfg.Port,
		numConns: cfg.NumConns,
		connCfg: ConnConfig{
			ProtoVersion:  cfg.ProtoVersion,
			CQLVersion:    cfg.CQLVersion,
			Timeout:       cfg.Timeout,
			NumStreams:    cfg.NumStreams,
			Compressor:    cfg.Compressor,
			Authenticator: cfg.Authenticator,
			Keepalive:     cfg.SocketKeepalive,
			SslOpts:       cfg.SslOpts,
		},
		keyspace:      cfg.Keyspace,
		hostPolicy:    hostPolicy,
		connPolicy:    connPolicy,
		hostConnPools: map[string]*hostConnPool{},
	}

	hosts := make([]*HostInfo, len(cfg.Hosts))
	for i, hostAddr := range cfg.Hosts {
		hosts[i] = &HostInfo{Peer: hostAddr}
	}

	pool.SetHosts(hosts, "")

	return pool
}

func (p *policyConnPool) SetHosts(hosts []*HostInfo, partitioner string) {
	p.mu.Lock()

	toRemove := make(map[string]struct{})
	for addr := range p.hostConnPools {
		toRemove[addr] = struct{}{}
	}

	// TODO connect to hosts in parallel, but wait for pools to be
	// created before returning

	for _, host := range hosts {
		pool, exists := p.hostConnPools[host.Peer]
		if !exists {
			// create a connection pool for the host
			pool = newHostConnPool(
				host.Peer,
				p.port,
				p.numConns,
				p.connCfg,
				p.keyspace,
				p.connPolicy(),
			)
			p.hostConnPools[host.Peer] = pool
		} else {
			// still have this host, so don't remove it
			delete(toRemove, host.Peer)
		}
	}

	for addr := range toRemove {
		pool := p.hostConnPools[addr]
		delete(p.hostConnPools, addr)
		pool.Close()
	}

	// update the policy
	p.hostPolicy.SetHosts(hosts, partitioner)

	p.mu.Unlock()
}

func (p *policyConnPool) Size() int {
	p.mu.RLock()
	count := 0
	for _, pool := range p.hostConnPools {
		count += pool.Size()
	}
	p.mu.RUnlock()

	return count
}

func (p *policyConnPool) Pick(qry *Query) *Conn {
	nextHost := p.hostPolicy.Pick(qry)

	p.mu.RLock()
	var host *HostInfo
	var conn *Conn
	for conn == nil {
		host = nextHost()
		if host == nil {
			break
		}
		conn = p.hostConnPools[host.Peer].Pick(qry)
	}
	p.mu.RUnlock()
	return conn
}

func (p *policyConnPool) Close() {
	p.mu.Lock()
	// remove the hosts from the policy
	p.hostPolicy.SetHosts([]*HostInfo{}, "")
	// close the pools
	for addr, pool := range p.hostConnPools {
		delete(p.hostConnPools, addr)
		pool.Close()
	}
	p.mu.Unlock()
}

type hostConnPool struct {
	host     string
	port     int
	addr     string
	size     int
	connCfg  ConnConfig
	keyspace string
	policy   ConnSelectionPolicy
	// protection for conns, closed, filling
	mu      sync.Mutex
	conns   []*Conn
	closed  bool
	filling bool
}

func newHostConnPool(
	host string,
	port int,
	size int,
	connCfg ConnConfig,
	keyspace string,
	policy ConnSelectionPolicy,
) *hostConnPool {

	pool := &hostConnPool{
		host:     host,
		port:     port,
		addr:     JoinHostPort(host, port),
		size:     size,
		connCfg:  connCfg,
		keyspace: keyspace,
		policy:   policy,
		conns:    make([]*Conn, 0, size),
		filling:  false,
		closed:   false,
	}

	// fill the pool with the initial connections before returning
	pool.fill()

	return pool
}

func (pool *hostConnPool) Pick(qry *Query) *Conn {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil
	}

	empty := len(pool.conns) == 0
	pool.mu.Unlock()

	if empty {
		// try to fill the empty pool
		pool.fill()
	}

	return pool.policy.Pick(qry)
}

//Size returns the number of connections currently active in the pool
func (pool *hostConnPool) Size() int {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	return len(pool.conns)
}

func (pool *hostConnPool) Close() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.closed {
		return
	}
	pool.closed = true

	// drain, but don't wait
	go pool.drain()
}

func (pool *hostConnPool) fill() {
	pool.mu.Lock()
	// avoid filling a closed pool, or concurrent filling
	if pool.closed || pool.filling {
		pool.mu.Unlock()
		return
	}

	// determine the filling work to be done
	startCount := len(pool.conns)
	fillCount := pool.size - startCount

	// avoid filling a full (or overfull) pool
	if fillCount <= 0 {
		pool.mu.Unlock()
		return
	}

	// ok fill the pool
	pool.filling = true

	// allow others to access the pool while filling
	pool.mu.Unlock()

	// fill only the first connection synchronously
	if startCount == 0 {
		err := pool.connect()
		if opErr, ok := err.(*net.OpError); ok && opErr.Op == "read" {
			// connection refused
			// these are typical during a node outage so avoid log spam.
		} else if err != nil {
			log.Printf("error: failed to connect to %s - %v", pool.addr, err)
		}

		if err != nil {
			// probably unreachable host
			go pool.stopFilling()
			return
		}

		// filled one
		fillCount--
	}

	// fill the rest of the pool asynchronously
	go func() {
		for fillCount > 0 {
			err := pool.connect()
			if opErr, ok := err.(*net.OpError); ok && opErr.Op == "read" {
				// connection refused
				// these are typical during a node outage so avoid log spam.
			} else if err != nil {
				log.Printf("error: failed to connect to %s - %v", pool.addr, err)
			}

			// decrement, even on error
			fillCount--
		}

		// mark the end of filling
		pool.stopFilling()
	}()
}

func (pool *hostConnPool) stopFilling() {
	time.Sleep(100 * time.Millisecond)
	pool.mu.Lock()
	pool.filling = false
	pool.mu.Unlock()
}

// create a new connection to the host and add it to the pool
func (pool *hostConnPool) connect() error {
	// try to connect
	conn, err := Connect(pool.addr, pool.connCfg, pool.handleError)
	if err != nil {
		return err
	}

	if pool.keyspace != "" {
		// set the keyspace
		if err := conn.UseKeyspace(pool.keyspace); err != nil {
			conn.Close()
			return err
		}
	}

	// add the Conn to the pool
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.closed {
		conn.Close()
		return nil
	}

	pool.conns = append(pool.conns, conn)
	pool.policy.SetConns(pool.conns)
	return nil
}

// handle any error from a Conn
func (pool *hostConnPool) handleError(conn *Conn, err error, closed bool) {
	if !closed {
		// still an open connection, so continue using it
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	// find the connection index
	for i, candidate := range pool.conns {
		if candidate == conn {
			// remove the connection, not preserving order
			pool.conns[i], pool.conns = pool.conns[len(pool.conns)-1], pool.conns[:len(pool.conns)-1]

			// update the policy
			pool.policy.SetConns(pool.conns)

			// lost a connection, so fill the pool
			go pool.fill()
			break
		}
	}
}

// removes and closes all connections from the pool
func (pool *hostConnPool) drain() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// empty the pool
	conns := pool.conns
	pool.conns = pool.conns[:0]

	// update the policy
	pool.policy.SetConns(pool.conns)

	// close the connections
	for _, conn := range conns {
		conn.Close()
	}
}
