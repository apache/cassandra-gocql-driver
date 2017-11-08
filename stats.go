package gocql

import (
	"time"
)

// NoopXStater implements github.com/rs/xstats.XStater interface and noops
type NoopXStater struct{}

func (s *NoopXStater) AddTags(tags ...string) {}

func (s *NoopXStater) GetTags() []string {
	return []string{}
}

func (s *NoopXStater) Gauge(stat string, value float64, tags ...string) {}

func (s *NoopXStater) Count(stat string, count float64, tags ...string) {}

func (s *NoopXStater) Histogram(stat string, value float64, tags ...string) {}

func (s *NoopXStater) Timing(stat string, value time.Duration, tags ...string) {}
