package gocql

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type nodeState int32

func (n nodeState) String() {
	if n == NodeUp {
		return "UP"
	} else if n == NodeDown {
		return "DOWN"
	}
}

const (
	NodeUp nodeState = iota
	NodeDown
)

type cassVersion struct {
	Major, Minor, Patch int
}

func (c cassVersion) String() string {
	return fmt.Sprintf("v%d.%d.%d", c.Major, c.Minor, c.Patch)
}

type HostInfo struct {
	// TODO(zariel): reduce locking maybe, not all values will change, but to ensure
	// that we are thread safe use a mutex to access all fields.
	mu         sync.RWMutex
	peer       string
	dataCenter string
	rack       string
	hostId     string
	version    cassVersion
	state      nodeState
	tokens     []string
}

func (h *HostInfo) Peer() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.peer
}

func (h *HostInfo) SetPeer(peer string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peer = peer
	return h
}

func (h *HostInfo) DataCenter() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.dataCenter
}

func (h *HostInfo) SetDataCenter(dataCenter string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dataCenter = dataCenter
	return h
}

func (h *HostFilter) Rack() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rack
}

func (h *HostInfo) SetRack(rack string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rack = rack
	return h
}

func (h *HostInfo) HostID() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.hostId
}

func (h *HostInfo) SetHostID(hostID string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hostId = hostID
	return h
}

func (h *HostInfo) Version() cassVersion {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.version
}

func (h *HostInfo) SetVersion(major, minor, patch int) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.version.Major = major
	h.version.Minor = minor
	h.version.Patch = patch
	return h
}

func (h *HostInfo) State() nodeState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

func (h *HostInfo) SetState(state nodeState) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state = state
	return h
}

func (h *HostInfo) Tokens() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.tokens
}

func (h *HostInfo) SetTokens(tokens []string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tokens = tokens
	return h
}

func (h HostInfo) String() string {
	return fmt.Sprintf("[hostinfo peer=%q data_centre=%q rack=%q host_id=%q version=%q state=%s num_tokens=%d]", h.Peer, h.DataCenter, h.Rack, h.HostId, h.Version, h.State, len(h.Tokens))
}

// Polls system.peers at a specific interval to find new hosts
type ringDescriber struct {
	dcFilter        string
	rackFilter      string
	prevHosts       []HostInfo
	prevPartitioner string
	session         *Session
	closeChan       chan bool
	// indicates that we can use system.local to get the connections remote address
	localHasRpcAddr bool

	mu sync.Mutex
}

func checkSystemLocal(control *controlConn) (bool, error) {
	iter := control.query("SELECT broadcast_address FROM system.local")
	if err := iter.err; err != nil {
		if errf, ok := err.(*errorFrame); ok {
			if errf.code == errSyntax {
				return false, nil
			}
		}

		return false, err
	}

	return true, nil
}

func (r *ringDescriber) GetHosts() (hosts []HostInfo, partitioner string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// we need conn to be the same because we need to query system.peers and system.local
	// on the same node to get the whole cluster

	const (
		legacyLocalQuery = "SELECT data_center, rack, host_id, tokens, partitioner FROM system.local"
		// only supported in 2.2.0, 2.1.6, 2.0.16
		localQuery = "SELECT broadcast_address, data_center, rack, host_id, tokens, partitioner FROM system.local"
	)

	var localHost HostInfo
	if r.localHasRpcAddr {
		iter := r.session.control.query(localQuery)
		if iter == nil {
			return r.prevHosts, r.prevPartitioner, nil
		}

		iter.Scan(&localHost.Peer, &localHost.DataCenter, &localHost.Rack,
			&localHost.HostId, &localHost.Tokens, &partitioner)

		if err = iter.Close(); err != nil {
			return nil, "", err
		}
	} else {
		iter := r.session.control.query(legacyLocalQuery)
		if iter == nil {
			return r.prevHosts, r.prevPartitioner, nil
		}

		iter.Scan(&localHost.DataCenter, &localHost.Rack, &localHost.HostId, &localHost.Tokens, &partitioner)

		if err = iter.Close(); err != nil {
			return nil, "", err
		}

		addr, _, err := net.SplitHostPort(r.session.control.addr())
		if err != nil {
			// this should not happen, ever, as this is the address that was dialed by conn, here
			// a panic makes sense, please report a bug if it occurs.
			panic(err)
		}

		localHost.Peer = addr
	}

	hosts = []HostInfo{localHost}

	iter := r.session.control.query("SELECT rpc_address, data_center, rack, host_id, tokens FROM system.peers")
	if iter == nil {
		return r.prevHosts, r.prevPartitioner, nil
	}

	host := HostInfo{}
	for iter.Scan(&host.Peer, &host.DataCenter, &host.Rack, &host.HostId, &host.Tokens) {
		if r.matchFilter(&host) {
			hosts = append(hosts, host)
		}
		host = HostInfo{}
	}

	if err = iter.Close(); err != nil {
		return nil, "", err
	}

	r.prevHosts = hosts
	r.prevPartitioner = partitioner

	return hosts, partitioner, nil
}

func (r *ringDescriber) matchFilter(host *HostInfo) bool {

	if r.dcFilter != "" && r.dcFilter != host.DataCenter {
		return false
	}

	if r.rackFilter != "" && r.rackFilter != host.Rack {
		return false
	}

	return true
}

func (r *ringDescriber) refreshRing() {
	// if we have 0 hosts this will return the previous list of hosts to
	// attempt to reconnect to the cluster otherwise we would never find
	// downed hosts again, could possibly have an optimisation to only
	// try to add new hosts if GetHosts didnt error and the hosts didnt change.
	hosts, partitioner, err := r.GetHosts()
	if err != nil {
		log.Println("RingDescriber: unable to get ring topology:", err)
		return
	}

	r.session.pool.SetHosts(hosts)
	r.session.pool.SetPartitioner(partitioner)
}

func (r *ringDescriber) run(sleep time.Duration) {
	if sleep == 0 {
		sleep = 30 * time.Second
	}

	for {
		r.refreshRing()

		select {
		case <-time.After(sleep):
		case <-r.closeChan:
			return
		}
	}
}
