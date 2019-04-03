package gocql

import (
	"encoding/hex"
	"sync"

	"github.com/gocql/gocql/internal/lru"
	"github.com/minio/highwayhash"
)

const (
	defaultMaxPreparedStmts = 1000
	shardCount              = 256
)

// preparedLRU is the prepared statement cache
type preparedLRU struct {
	shards [shardCount]*shard
	key    []byte
}

type shard struct {
	items *lru.Cache
	lock  *sync.RWMutex
}

// newPreparedLRU creates and initializes a * preparedLRU instance.
func newPreparedLRU(size int) *preparedLRU {
	key, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000")
	if err != nil {
		panic(err)
	}
	c := &preparedLRU{
		key: key,
	}
	for i := 0; i < shardCount; i++ {
		c.shards[i] = &shard{
			items: lru.New(size),
			lock:  new(sync.RWMutex),
		}
	}
	return c
}

// Max adjusts the maximum size of the cache and cleans up the oldest records if
// the new max is lower than the previous value. Not concurrency safe.
func (p *preparedLRU) max(max int) {
	for _, shard := range p.shards {
		shard.lock.Lock()
		for shard.items.Len() > max {
			shard.items.RemoveOldest()
		}
		shard.items.MaxEntries = max
		shard.lock.Unlock()
	}
}

func (p *preparedLRU) clear() {
	for _, shard := range p.shards {
		shard.lock.Lock()
		if shard.items.Len() > 0 {
			shard.items.RemoveOldest()
		}
		shard.lock.Unlock()
	}
}

func (p *preparedLRU) add(key string, val *inflightPrepare) {
	shard := p.shard(key)
	shard.lock.Lock()
	shard.items.Add(key, val)
	shard.lock.Unlock()
}

func (p *preparedLRU) remove(key string) bool {
	shard := p.shard(key)
	shard.lock.Lock()
	ok := shard.items.Remove(key)
	shard.lock.Unlock()
	return ok
}

func (p *preparedLRU) execIfMissing(key string, fn func(lru *lru.Cache) *inflightPrepare) (*inflightPrepare, bool) {
	shard := p.shard(key)
	shard.lock.Lock()
	defer shard.lock.Unlock()

	val, ok := shard.items.Get(key)
	if ok {
		return val.(*inflightPrepare), true
	}

	return fn(shard.items), false
}

func (p *preparedLRU) keyFor(addr, keyspace, statement string) string {
	// TODO: maybe use []byte for keys?
	return addr + keyspace + statement
}

func (c *preparedLRU) get(key string) *inflightPrepare {
	shard := c.shard(key)
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if inflight, ok := shard.items.Get(key); ok {
		return inflight.(*inflightPrepare)
	}
	return nil
}

func (c *preparedLRU) set(key string, val *inflightPrepare) {
	shard := c.shard(key)
	shard.lock.Lock()
	shard.items.Add(key, val)
	shard.lock.Unlock()
}

func (c *preparedLRU) shard(key string) (shard *shard) {
	shardKey := highwayhash.Sum64([]byte(key), c.key) % shardCount
	return c.shards[shardKey]
}
