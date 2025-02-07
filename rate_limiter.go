package gocql

import (
	"sync"
	"time"
)

// RateLimiterConfig holds the configuration parameters for the rate limiter, which uses Token Bucket approach.
//
// Fields:
//
// - rate: Allowed requests per second
// - Burst: Maximum number of burst requests
//
// Example:
//	RateLimiterConfig{
//		rate:  300000,
//		burst: 150,
//	}
type RateLimiterConfig struct {
	rate  float64
	burst int
}

type tokenBucket struct {
	rate         float64
	burst        int
	tokens       int
	lastRefilled time.Time
	mu           sync.Mutex
}

func (tb *tokenBucket) refill() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	tokensToAdd := int(tb.rate * now.Sub(tb.lastRefilled).Seconds())
	tb.tokens = min(tb.tokens+tokensToAdd, tb.burst)
	tb.lastRefilled = now
}

func (tb *tokenBucket) Allow() bool {
	tb.refill()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type ConfigurableRateLimiter struct {
	tb tokenBucket
}

func NewConfigurableRateLimiter(rate float64, burst int) *ConfigurableRateLimiter {
	tb := tokenBucket{
		rate:         rate,
		burst:        burst,
		tokens:       burst,
		lastRefilled: time.Now(),
	}
	return &ConfigurableRateLimiter{tb}
}

func (rl *ConfigurableRateLimiter) Allow() bool {
	return rl.tb.Allow()
}
