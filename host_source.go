package gocql

import "time"

type hostInfo struct {
	peer       string
	dataCenter string
	rack       string
	hostId     string
	tokens     []string
}

// Polls system.peers at a specific interval to find new hosts
type ringDescriber struct {
	dcFilter   string
	rackFilter string
	previous   []string
	session    *Session
}

func (r *ringDescriber) GetHosts() []string {
	// we need conn to be the same because we need to query system.peers and system.local
	// on the same node to get the whole cluster
	conn := r.session.Pool.Pick(nil)
	if conn == nil {
		return r.previous
	}

	// TODO: Get conn's tokens form system.local
	query := r.session.Query("SELECT peer, data_center, rack, host_id, tokens FROM system.peers")
	iter := conn.executeQuery(query)

	hosts := []string{conn.Address()}
	host := hostInfo{}

	for iter.Scan(&host.peer, &host.dataCenter, &host.rack, &host.hostId, &host.tokens) {
		if r.matchFilter(&host) {
			// TODO: Capture tokens
			hosts = append(hosts, host.peer)
		}
	}

	if err := iter.Close(); err != nil {
		return r.previous
	}

	r.previous = hosts

	return hosts
}

func (r *ringDescriber) matchFilter(host *hostInfo) bool {
	if r.dcFilter == "" && r.rackFilter == "" {
		return true
	}

	if r.dcFilter != "" && r.dcFilter != host.dataCenter {
		return false
	}

	if r.rackFilter != "" && r.rackFilter != host.rack {
		return false
	}

	return true
}

func (h *ringDescriber) run(sleep time.Duration) {
	if sleep == 0 {
		sleep = 30 * time.Second
	}

	prev := make(map[string]struct{})
	for {
		// if we have 0 hosts this will return the previous list of hosts to
		// attempt to reconnect to the cluster otherwise we would never find
		// downed hosts again, could possibly have an optimisation to only
		// try to add new hosts if GetHosts didnt error and the hosts didnt change.
		hosts := h.GetHosts()
		current := make(map[string]struct{})
		for _, host := range hosts {
			if _, ok := prev[host]; !ok {
				h.session.Pool.AddHost(host)
			} else {
				delete(prev, host)
			}

			current[host] = struct{}{}
		}

		for host := range prev {
			h.session.Pool.RemoveHost(host)
		}

		prev = current

		time.Sleep(sleep)
	}
}
