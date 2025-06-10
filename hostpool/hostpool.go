package hostpool

import (
	"sync"

	"github.com/hailocab/go-hostpool"

	"github.com/gocql/gocql"
)

// HostPoolHostPolicy is a host policy which uses the bitly/go-hostpool library
// to distribute queries between hosts and prevent sending queries to
// unresponsive hosts. When creating the host pool that is passed to the policy
// use an empty slice of hosts as the hostpool will be populated later by gocql.
// See below for examples of usage:
//
//	// Create host selection policy using a simple host pool
//	cluster.PoolConfig.HostSelectionPolicy = HostPoolHostPolicy(hostpool.New(nil))
//
//	// Create host selection policy using an epsilon greedy pool
//	cluster.PoolConfig.HostSelectionPolicy = HostPoolHostPolicy(
//	    hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
//	)
func HostPoolHostPolicy(hp hostpool.HostPool) *hostPoolHostPolicy {
	return &hostPoolHostPolicy{hostMap: map[string]*gocql.HostInfo{}, hp: hp}
}

type hostPoolHostPolicy struct {
	hp      hostpool.HostPool
	mu      sync.RWMutex
	hostMap map[string]*gocql.HostInfo
}

func (r *hostPoolHostPolicy) Init(*gocql.Session)                       {}
func (r *hostPoolHostPolicy) KeyspaceChanged(gocql.KeyspaceUpdateEvent) {}
func (r *hostPoolHostPolicy) SetPartitioner(string)                     {}
func (r *hostPoolHostPolicy) IsLocal(*gocql.HostInfo) bool              { return true }

func (r *hostPoolHostPolicy) SetHosts(hosts []*gocql.HostInfo) {
	peers := make([]string, len(hosts))
	hostMap := make(map[string]*gocql.HostInfo, len(hosts))

	for i, host := range hosts {
		ip := host.ConnectAddress().String()
		peers[i] = ip
		hostMap[ip] = host
	}

	r.mu.Lock()
	r.hp.SetHosts(peers)
	r.hostMap = hostMap
	r.mu.Unlock()
}

func (r *hostPoolHostPolicy) AddHost(host *gocql.HostInfo) {
	ip := host.ConnectAddress().String()

	r.mu.Lock()
	defer r.mu.Unlock()

	// If the host addr is present and isn't nil return
	if h, ok := r.hostMap[ip]; ok && h != nil {
		return
	}
	// otherwise, add the host to the map
	r.hostMap[ip] = host
	// and construct a new peer list to give to the HostPool
	hosts := make([]string, 0, len(r.hostMap))
	for addr := range r.hostMap {
		hosts = append(hosts, addr)
	}

	r.hp.SetHosts(hosts)
}

func (r *hostPoolHostPolicy) RemoveHost(host *gocql.HostInfo) {
	ip := host.ConnectAddress().String()

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.hostMap[ip]; !ok {
		return
	}

	delete(r.hostMap, ip)
	hosts := make([]string, 0, len(r.hostMap))
	for _, host := range r.hostMap {
		hosts = append(hosts, host.ConnectAddress().String())
	}

	r.hp.SetHosts(hosts)
}

func (r *hostPoolHostPolicy) HostUp(host *gocql.HostInfo) {
	r.AddHost(host)
}

func (r *hostPoolHostPolicy) HostDown(host *gocql.HostInfo) {
	r.RemoveHost(host)
}

func (r *hostPoolHostPolicy) Pick(qry gocql.ExecutableStatement) gocql.NextHost {
	return func() gocql.SelectedHost {
		r.mu.RLock()
		defer r.mu.RUnlock()

		if len(r.hostMap) == 0 {
			return nil
		}

		hostR := r.hp.Get()
		host, ok := r.hostMap[hostR.Host()]
		if !ok {
			return nil
		}

		return selectedHostPoolHost{
			policy: r,
			info:   host,
			hostR:  hostR,
		}
	}
}

// selectedHostPoolHost is a host returned by the hostPoolHostPolicy and
// implements the SelectedHost interface
type selectedHostPoolHost struct {
	policy *hostPoolHostPolicy
	info   *gocql.HostInfo
	hostR  hostpool.HostPoolResponse
}

func (host selectedHostPoolHost) Info() *gocql.HostInfo {
	return host.info
}

func (host selectedHostPoolHost) Mark(err error) {
	ip := host.info.ConnectAddress().String()

	host.policy.mu.RLock()
	defer host.policy.mu.RUnlock()

	if _, ok := host.policy.hostMap[ip]; !ok {
		// host was removed between pick and mark
		return
	}

	host.hostR.Mark(err)
}
