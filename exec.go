package gocql

import (
	"fmt"
)

// SingleHostQueryExecutor allows to quickly execute diagnostic queries while
// connected to only a single node.
// The executor opens only a single connection to a node and does not use
// connection pools.
// Consistency level used is ONE.
// Retry policy is applied, attempts are visible in query metrics but query
// observer is not notified.
type SingleHostQueryExecutor struct {
	session *Session
	control *controlConn
}

// Exec executes the query without returning any rows.
func (e SingleHostQueryExecutor) Exec(stmt string, values ...interface{}) error {
	return e.control.query(stmt, values...).Close()
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (e SingleHostQueryExecutor) Iter(stmt string, values ...interface{}) *Iter {
	return e.control.query(stmt, values...)
}

func (e SingleHostQueryExecutor) Close() {
	if e.control != nil {
		e.control.close()
	}
	if e.session != nil {
		e.session.Close()
	}
}

// NewSingleHostQueryExecutor creates a SingleHostQueryExecutor by connecting
// to one of the hosts specified in the ClusterConfig.
// If ProtoVersion is not specified version 4 is used.
// Caller is responsible for closing the executor after use.
func NewSingleHostQueryExecutor(cfg *ClusterConfig) (e SingleHostQueryExecutor, err error) {
	// Check that hosts in the ClusterConfig is not empty
	if len(cfg.Hosts) < 1 {
		err = ErrNoHosts
		return
	}

	c := *cfg

	// If protocol version not set assume 4 and skip discovery
	if c.ProtoVersion == 0 {
		e.session.logger.Print("gocql: setting protocol version 4")
		c.ProtoVersion = 4
	}

	// Close in case of error
	defer func() {
		if err != nil {
			e.Close()
		}
	}()

	// Create uninitialised session
	c.disableInit = true
	if e.session, err = NewSession(c); err != nil {
		err = fmt.Errorf("new session: %w", err)
		return
	}

	var hosts []*HostInfo
	if hosts, err = addrsToHosts(c.Hosts, c.Port, c.Logger); err != nil {
		err = fmt.Errorf("addrs to hosts: %w", err)
		return
	}

	// Create control connection to one of the hosts
	e.control = createControlConn(e.session)

	// shuffle endpoints so not all drivers will connect to the same initial
	// node.
	hosts = shuffleHosts(hosts)

	conncfg := *e.control.session.connCfg
	conncfg.disableCoalesce = true

	var conn *Conn

	for _, host := range hosts {
		conn, err = e.control.session.dial(e.control.session.ctx, host, &conncfg, e.control)
		if err != nil {
			e.control.session.logger.Printf("gocql: unable to dial control conn %v:%v: %v\n", host.ConnectAddress(), host.Port(), err)
			continue
		}
		err = e.control.setupConn(conn)
		if err == nil {
			break
		}
		e.control.session.logger.Printf("gocql: unable setup control conn %v:%v: %v\n", host.ConnectAddress(), host.Port(), err)
		conn.Close()
		conn = nil
	}

	if conn == nil {
		err = fmt.Errorf("setup: %w", err)
		return
	}

	return
}
