// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"time"
)

// ClusterConfig is a struct to configure the default cluster implementation
// of gocoql. It has a varity of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that requre a
// different setup must implement their own cluster.
type ClusterConfig struct {
	Hosts           []string      // addresses for the initial connections
	CQLVersion      string        // CQL version (default: 3.0.0)
	ProtoVersion    int           // version of the native protocol (default: 2)
	Timeout         time.Duration // connection timeout (default: 600ms)
	DefaultPort     int           // default port (default: 9042)
	Keyspace        string        // initial keyspace (optional)
	NumConns        int           // number of connections per host (default: 2)
	NumStreams      int           // number of streams per connection (default: 128)
	Consistency     Consistency   // default consistency level (default: Quorum)
	Compressor      Compressor    // compression algorithm (default: nil)
	Authenticator   Authenticator // authenticator (default: nil)
	RetryPolicy     RetryPolicy   // Default retry policy to use for queries (default: 0)
	SocketKeepalive time.Duration // The keepalive period to use, enabled if > 0 (default: 0)
	ConnPoolType    NewPoolFunc   // The function used to create the connection pool for the session (default: NewSimplePool)
}

// NewCluster generates a new config for the default cluster implementation.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:        hosts,
		CQLVersion:   "3.0.0",
		ProtoVersion: 2,
		Timeout:      600 * time.Millisecond,
		DefaultPort:  9042,
		NumConns:     2,
		NumStreams:   128,
		Consistency:  Quorum,
		ConnPoolType: NewSimplePool,
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
	pool := cfg.ConnPoolType(cfg)

	//See if there are any connections in the pool
	if pool.Size() > 0 {
		s := NewSession(pool, *cfg)
		s.SetConsistency(cfg.Consistency)
		return s, nil
	}

	pool.Close()
	return nil, ErrNoConnectionsStarted

}

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
)
