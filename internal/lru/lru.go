/*
Copyright 2015 To gocql authors
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package lru implements an LRU cache.
package lru

import (
	"container/list"
	"sync"
	"time"
)

// Cache is an LRU cache. It is safe for concurrent access.
//
// This cache has been forked from github.com/golang/groupcache/lru, but
// specialized with string keys to avoid the allocations caused by wrapping them
// in interface{}.
type Cache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key string, value interface{})

	window time.Duration
	ll     *list.List
	cache  map[string]*list.Element
	mu     sync.RWMutex
}

type entry struct {
	key       string
	value     interface{}
	timestamp time.Time
}

// New creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
// If window is zero the "windowed updates" of the LRU list will be disabled.
// For further information on this technique see: https://www.openmymind.net/High-Concurrency-LRU-Caching/
func New(maxEntries int, window time.Duration) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
		window:     window,
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key string, val interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).value = val
		return
	}
	c.addLocked(key, val)
}

func (c *Cache) addLocked(key string, val interface{}) {
	timestamp := time.Now()
	ent := &entry{key, val, timestamp}
	ele := c.ll.PushFront(ent)
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.removeOldestLocked()
	}
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key string) (value interface{}, ok bool) {
	timestamp := time.Now()

	if c.window == 0 {
		return c.getWithLock(key, timestamp)
	}

	c.mu.RLock()
	if ele, ok := c.cache[key]; ok {
		ent := ele.Value.(*entry)
		if timestamp.After(ent.timestamp.Add(c.window)) {
			c.mu.RUnlock()
			// We dropped the lock so we need to perform the lookup again.
			return c.getWithLock(key, timestamp)
		} else {
			v := ent.value
			c.mu.RUnlock()
			return v, true
		}
	}
	c.mu.RUnlock()
	return
}

func (c *Cache) getWithLock(key string, timestamp time.Time) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.cache[key]; ok {
		ent := ele.Value.(*entry)
		ent.timestamp = timestamp
		c.ll.MoveToFront(ele)
		return ent.value, true
	} else {
		return nil, false
	}
}

// GetOrInsert either retrieves the current entry for the key or inserts the
// provided value for the key.
// It returns true if the value was loaded and false if the provided value
// was stored.
func (c *Cache) GetOrInsert(key string, val interface{}) (interface{}, bool) {
	// Optimistically try a Get first
	value, ok := c.Get(key)
	if ok {
		return value, ok
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// We need to check again
	if ele, ok := c.cache[key]; ok {
		return ele.Value.(*entry).value, true
	}

	c.addLocked(key, val)
	return val, false
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	c.mu.RLock()
	l := c.ll.Len()
	c.mu.RUnlock()
	return l
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		c.removeElementLocked(ele)
		return true
	}

	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeOldestLocked()
}

func (c *Cache) removeOldestLocked() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElementLocked(ele)
	}
}

func (c *Cache) removeElementLocked(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}
