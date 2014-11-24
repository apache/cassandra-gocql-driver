package gocql

import (
	"log"
	"net"
	"time"
)

type HostInfo struct {
	Peer       string
	DataCenter string
	Rack       string
	HostId     string
	Tokens     []string
}

// Polls system.peers at a specific interval to find new hosts
type ringDescriber struct {
	dcFilter   string
	rackFilter string
	previous   []HostInfo
	session    *Session
}

func (r *ringDescriber) GetHosts() ([]HostInfo, error) {
	// we need conn to be the same because we need to query system.peers and system.local
	// on the same node to get the whole cluster
	conn := r.session.Pool.Pick(nil)
	if conn == nil {
		return r.previous, nil
	}

	query := r.session.Query("SELECT data_center, rack, host_id, tokens FROM system.local")
	iter := conn.executeQuery(query)

	host := &HostInfo{}
	iter.Scan(&host.DataCenter, &host.Rack, &host.HostId, &host.Tokens)

	if err := iter.Close(); err != nil {
		return nil, err
	}

	addr, _, err := net.SplitHostPort(conn.Address())
	if err != nil {
		// this should not happen, ever, as this is the address that was dialed by conn, here
		// a panic makes sense, please report a bug if it occurs.
		panic(err)
	}

	host.Peer = addr

	hosts := []HostInfo{*host}

	query = r.session.Query("SELECT peer, data_center, rack, host_id, tokens FROM system.peers")
	iter = conn.executeQuery(query)

	for iter.Scan(&host.Peer, &host.DataCenter, &host.Rack, &host.HostId, &host.Tokens) {
		if r.matchFilter(host) {
			hosts = append(hosts, *host)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	r.previous = hosts

	return hosts, nil
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

func (h *ringDescriber) run(sleep time.Duration) {
	if sleep == 0 {
		sleep = 30 * time.Second
	}

	for {
		// if we have 0 hosts this will return the previous list of hosts to
		// attempt to reconnect to the cluster otherwise we would never find
		// downed hosts again, could possibly have an optimisation to only
		// try to add new hosts if GetHosts didnt error and the hosts didnt change.
		hosts, err := h.GetHosts()
		if err != nil {
			log.Println("RingDescriber: unable to get ring topology:", err)
		} else {
			h.session.Pool.SetHosts(hosts)
		}

		time.Sleep(sleep)
	}
}
