package gocql

import (
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql/internal/lru"
)

const defaultMaxPreparedStmts = 1000

type preparedCache struct {
	hosts     atomic.Value // map[string]*preparedLRU
	mu        sync.Mutex
	maxStmnts int
}

func (p *preparedCache) load() map[string]*preparedLRU {
	v, _ := p.hosts.Load().(map[string]*preparedLRU)
	return v
}

func (p *preparedCache) clear() {
	p.mu.Lock()
	p.hosts.Store(make(map[string]*preparedLRU))
	p.mu.Unlock()
}

func (p *preparedCache) forHost(addr string) *preparedLRU {
	m := p.load()
	lru, ok := m[addr]
	if !ok {
		p.mu.Lock()
		m := p.load()
		lru, ok = m[addr]
		if !ok {
			lru = p.addHost(m, addr)
		}
		p.mu.Unlock()
	}

	return lru
}

func (p *preparedCache) addHost(current map[string]*preparedLRU, addr string) *preparedLRU {
	cow := make(map[string]*preparedLRU, len(current)+1)
	for k, v := range current {
		cow[k] = v
	}

	lru := &preparedLRU{lru: lru.New(p.maxStmnts)}
	cow[addr] = lru
	p.hosts.Store(cow)
	return lru
}

func (p *preparedCache) HostUp(host *HostInfo) {
	addr := JoinHostPort(host.ConnectAddress().String(), host.Port())

	v := p.load()
	if _, ok := v[addr]; ok {
		return
	}

	cow := make(map[string]*preparedLRU, len(v)+1)
	for k, v := range v {
		cow[k] = v
	}

	cow[addr] = &preparedLRU{lru: lru.New(p.maxStmnts)}
	p.hosts.Store(cow)
}

func (p *preparedCache) HostDown(host *HostInfo) {
	addr := JoinHostPort(host.ConnectAddress().String(), host.Port())

	p.mu.Lock()
	defer p.mu.Unlock()

	v := p.load()
	if _, ok := v[addr]; !ok {
		return
	}

	cow := make(map[string]*preparedLRU, len(v)-1)
	for k, v := range v {
		if k != addr {
			cow[k] = v
		}
	}

	p.hosts.Store(cow)
}

// preparedLRU is the prepared statement cache
type preparedLRU struct {
	mu  sync.Mutex
	lru *lru.Cache
}

func (p *preparedLRU) add(key string, val *inflightPrepare) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lru.Add(key, val)
}

func (p *preparedLRU) remove(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lru.Remove(key)
}

func (p *preparedLRU) execIfMissing(key string, fn func(lru *lru.Cache) *inflightPrepare) (*inflightPrepare, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	val, ok := p.lru.Get(key)
	if ok {
		return val.(*inflightPrepare), true
	}

	return fn(p.lru), false
}

func (p *preparedLRU) keyFor(keyspace, statement string) string {
	// TODO: maybe use []byte for keys?
	return keyspace + statement
}
