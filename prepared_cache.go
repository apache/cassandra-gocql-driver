package gocql

import (
	"context"
	"sync"

	"github.com/gocql/gocql/internal/lru"
)

const defaultMaxPreparedStmts = 1000

// preparedLRU is the prepared statement cache
type preparedLRU struct {
	mu  sync.Mutex
	lru *lru.Cache
}

func newPreparedLRU(maxEntries int) *preparedLRU {
	lru := lru.New(maxEntries)
	lru.OnEvicted = func(key string, value interface{}) {
		value.(*inflightPrepare).removed = true
	}
	return &preparedLRU{lru: lru}
}

// Max adjusts the maximum size of the cache and cleans up the oldest records if
// the new max is lower than the previous value. Not concurrency safe.
func (p *preparedLRU) max(max int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.lru.Len() > max {
		p.lru.RemoveOldest()
	}
	p.lru.MaxEntries = max
}

func (p *preparedLRU) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.lru.Len() > 0 {
		p.lru.RemoveOldest()
	}
}

func (p *preparedLRU) add(key string, val *inflightPrepare) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lru.Add(key, val)
}

func (p *preparedLRU) remove(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if val, ok := p.lru.Get(key); ok {
		val.(*inflightPrepare).removed = true
	}
	return p.lru.Remove(key)
}

// enterFlight returns an *inflightPrepare that coordinates context for doing deduplicated work.
// It returns the flight object itself.
// In case the flight was just created, it will run fn in a separate goroutine, with a background context
// that will be canceled only when nobody waits for the result anymore or when the work is done.
func (p *preparedLRU) enterFlight(key string, fn func(context.Context) (*preparedStatment, error)) *inflightPrepare {
	p.mu.Lock()
	defer p.mu.Unlock()

	val, ok := p.lru.Get(key)
	if ok {
		flight := val.(*inflightPrepare)
		flight.waiterCount++
		return flight
	}

	flight := new(inflightPrepare)
	flight.done = make(chan struct{})
	flight.waiterCount = 1
	ctx, cancel := context.WithCancel(context.Background())
	flight.cancel = cancel

	p.lru.Add(key, flight)

	go func() {
		preparedStatement, err := fn(ctx)

		p.mu.Lock()
		defer p.mu.Unlock()

		flight.preparedStatment, flight.err = preparedStatement, err
		if flight.err != nil && !flight.removed {
			p.lru.Remove(key)
		}
		flight.cancel()
		close(flight.done)
		flight.isDone = true
	}()

	return flight
}

// leaveFlight is called when waiter no longer wants to wait for the result of the flight.
func (p *preparedLRU) leaveFlight(key string, flight *inflightPrepare) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flight.waiterCount--

	if !flight.isDone && flight.waiterCount == 0 {
		// nobody wants to wait for unfinished flight, let's cancel it.
		if !flight.removed {
			p.lru.Remove(key)
		}
		flight.cancel()
	}
}

func (p *preparedLRU) keyFor(addr, keyspace, statement string) string {
	// TODO: maybe use []byte for keys?
	return addr + keyspace + statement
}

type inflightPrepare struct {
	// done is closed after the work for this flight is done and err and preparedStatement have been set.
	done chan struct{}
	// isDone indicates whether the work for this flight is done (i.e. if the done channel was closed)
	isDone bool

	// preparedStatement and err are the result returned from the work function
	preparedStatment *preparedStatment
	err error

	// waiterCount is used to keep track of how many goroutines wait for the result of this flight.
	waiterCount int64
	// removed is true iff the flight has been removed from the cache
	removed bool
	// cancel is cancellation function of the context used to call work function.
	cancel context.CancelFunc
}
