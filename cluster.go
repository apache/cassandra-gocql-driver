// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

const defaultMaxPreparedStmts = 1000

//Package global reference to Prepared Statements LRU
var stmtsLRU preparedLRU

//preparedLRU is the prepared statement cache
type preparedLRU struct {
	sync.Mutex
	lru *lru.Cache
}

//Max adjusts the maximum size of the cache and cleans up the oldest records if
//the new max is lower than the previous value. Not concurrency safe.
func (p *preparedLRU) Max(max int) {
	for p.lru.Len() > max {
		p.lru.RemoveOldest()
	}
	p.lru.MaxEntries = max
}

func initStmtsLRU(max int) {
	if stmtsLRU.lru != nil {
		stmtsLRU.Max(max)
	} else {
		stmtsLRU.lru = lru.New(max)
	}
}

// To enable periodic node discovery enable DiscoverHosts in ClusterConfig
type DiscoveryConfig struct {
	// If not empty will filter all discoverred hosts to a single Data Centre (default: "")
	DcFilter string
	// If not empty will filter all discoverred hosts to a single Rack (default: "")
	RackFilter string
	// The interval to check for new hosts (default: 30s)
	Sleep time.Duration
}

// ClusterConfig is a struct to configure the default cluster implementation
// of gocoql. It has a varity of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that requre a
// different setup must implement their own cluster.
type ClusterConfig struct {
	Hosts             []string      // addresses for the initial connections
	CQLVersion        string        // CQL version (default: 3.0.0)
	ProtoVersion      int           // version of the native protocol (default: 2)
	Timeout           time.Duration // connection timeout (default: 600ms)
	Port              int           // port (default: 9042)
	Keyspace          string        // initial keyspace (optional)
	NumConns          int           // number of connections per host (default: 2)
	NumStreams        int           // number of streams per connection (default: max per protocol, either 128 or 32768)
	Consistency       Consistency   // default consistency level (default: Quorum)
	Compressor        Compressor    // compression algorithm (default: nil)
	Authenticator     Authenticator // authenticator (default: nil)
	RetryPolicy       RetryPolicy   // Default retry policy to use for queries (default: 0)
	SocketKeepalive   time.Duration // The keepalive period to use, enabled if > 0 (default: 0)
	ConnPoolType      NewPoolFunc   // The function used to create the connection pool for the session (default: NewSimplePool)
	DiscoverHosts     bool          // If set, gocql will attempt to automatically discover other members of the Cassandra cluster (default: false)
	MaxPreparedStmts  int           // Sets the maximum cache size for prepared statements globally for gocql (default: 1000)
	MaxRoutingKeyInfo int           // Sets the maximum cache size for query info about statements for each session (default: 1000)
	PageSize          int           // Default page size to use for created sessions (default: 0)
	Discovery         DiscoveryConfig
	SslOpts           *SslOptions
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:             hosts,
		CQLVersion:        "3.0.0",
		ProtoVersion:      2,
		Timeout:           600 * time.Millisecond,
		Port:              9042,
		NumConns:          2,
		Consistency:       Quorum,
		ConnPoolType:      NewSimplePool,
		DiscoverHosts:     false,
		MaxPreparedStmts:  defaultMaxPreparedStmts,
		MaxRoutingKeyInfo: 1000,
	}
	return cfg
}

// CreateSession initializes the cluster based on this config and returns a
// session object that can be used to interact with the database.
func (cfg *ClusterConfig) CreateSession() (*Session, error) {

	//Check that hosts in the ClusterConfig is not empty
	if len(cfg.Hosts) < 1 {
		return nil, ErrNoHosts
	}

	maxStreams := 128
	if cfg.ProtoVersion > protoVersion2 {
		maxStreams = 32768
	}

	if cfg.NumStreams <= 0 || cfg.NumStreams > maxStreams {
		cfg.NumStreams = maxStreams
	}

	pool, err := cfg.ConnPoolType(cfg)
	if err != nil {
		return nil, err
	}

	//Adjust the size of the prepared statements cache to match the latest configuration
	stmtsLRU.Lock()
	initStmtsLRU(cfg.MaxPreparedStmts)
	stmtsLRU.Unlock()

	//See if there are any connections in the pool
	if pool.Size() > 0 {
		s := NewSession(pool, *cfg)
		s.SetConsistency(cfg.Consistency)
		s.SetPageSize(cfg.PageSize)

		if cfg.DiscoverHosts {
			hostSource := &ringDescriber{
				session:    s,
				dcFilter:   cfg.Discovery.DcFilter,
				rackFilter: cfg.Discovery.RackFilter,
			}

			go hostSource.run(cfg.Discovery.Sleep)
		}

		return s, nil
	}

	pool.Close()
	return nil, ErrNoConnectionsStarted
}

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("unable to populate Hosts")
)
