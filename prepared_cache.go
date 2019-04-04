package gocql

import (
	"time"

	"github.com/gocql/gocql/internal/lru"
)

const (
	defaultMaxPreparedStmts    = 1000
	defaultPreparedStmtsWindow = time.Second
)

// preparedLRU is the prepared statement cache
type preparedLRU struct {
	lru *lru.Cache
}

// Max adjusts the maximum size of the cache and cleans up the oldest records if
// the new max is lower than the previous value. Not concurrency safe.
func (p *preparedLRU) max(max int) {
	for p.lru.Len() > max {
		p.lru.RemoveOldest()
	}
	p.lru.MaxEntries = max
}

func (p *preparedLRU) clear() {
	for p.lru.Len() > 0 {
		p.lru.RemoveOldest()
	}
}

func (p *preparedLRU) add(key string, val *inflightPrepare) {
	p.lru.Add(key, val)
}

func (p *preparedLRU) remove(key string) bool {
	return p.lru.Remove(key)
}

func (p *preparedLRU) execIfMissing(key string, fn func() *inflightPrepare) (*inflightPrepare, bool) {
	val, ok := p.lru.Get(key)
	if ok {
		return val.(*inflightPrepare), true
	}

	val, ok = p.lru.GetOrInsert(key, fn())
	return val.(*inflightPrepare), ok
}

func (p *preparedLRU) keyFor(addr, keyspace, statement string) string {
	// TODO: maybe use []byte for keys?
	return addr + keyspace + statement
}
