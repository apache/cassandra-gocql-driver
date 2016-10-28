package gocql

import (
	"testing"

	"github.com/gocql/gocql/internal/lru"
)

func BenchmarkLRU(b *testing.B) {
	pl := preparedLRU{
		lru: lru.New(10),
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pl.execIfMissing("foo", func(c *lru.Cache) *inflightPrepare {
				c.Add("foo", (*inflightPrepare)(nil))
				return nil
			})
		}
	})
}
