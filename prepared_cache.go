package gocql

import (
	"github.com/cornelk/hashmap"
)

const defaultMaxPreparedStmts = 1000

// preparedLRU is the prepared statement cache
type preparedLRU struct {
	cache *hashmap.HashMap
}

// Max adjusts the maximum size of the cache and cleans up the oldest records if
// the new max is lower than the previous value. Not concurrency safe.
func (p *preparedLRU) max(max int) {
	if p.cache.Len() > max {
		p.cache.Grow(uintptr(max))
	}
}

func (p *preparedLRU) clear() {
	for elem := range p.cache.Iter() {
		p.cache.Del(elem.Key)
	}
}

func (p *preparedLRU) add(key string, val *inflightPrepare) {
	p.cache.Insert(key, val)
}

func (p *preparedLRU) remove(key string) bool {
	p.cache.Del(key)
	return true
}

func (p *preparedLRU) execIfMissing(key string, fn func() *inflightPrepare) (*inflightPrepare, bool) {
	val := fn()
	v, ok := p.cache.GetOrInsert(key, val)
	return v.(*inflightPrepare), ok
}

func (p *preparedLRU) keyFor(addr, keyspace, statement string) string {
	// TODO: maybe use []byte for keys?
	return addr + keyspace + statement
}
