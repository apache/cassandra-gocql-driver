// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Cluster sets up and maintains the node configuration of a Cassandra
// cluster.
//
// It has a varity of attributes that can be used to modify the behavior
// to fit the most common use cases. Applications that requre a different
// a setup should compose the nodes on their own.
type Cluster struct {
	Hosts       []string
	CQLVersion  string
	Timeout     time.Duration
	DefaultPort int
	Keyspace    string
	ConnPerHost int
	DelayMin    time.Duration
	DelayMax    time.Duration

	pool     *RoundRobin
	initOnce sync.Once
	boot     sync.WaitGroup
	bootOnce sync.Once
}

func NewCluster(hosts ...string) *Cluster {
	c := &Cluster{
		Hosts:       hosts,
		CQLVersion:  "3.0.0",
		Timeout:     200 * time.Millisecond,
		DefaultPort: 9042,
	}
	return c
}

func (c *Cluster) init() {
	for i := 0; i < len(c.Hosts); i++ {
		addr := strings.TrimSpace(c.Hosts[i])
		if strings.IndexByte(addr, ':') < 0 {
			addr = fmt.Sprintf("%s:%d", addr, c.DefaultPort)
		}
		go c.connect(addr)
	}
	c.pool = NewRoundRobin()
	<-time.After(c.Timeout)
}

func (c *Cluster) connect(addr string) {
	delay := c.DelayMin
	for {
		conn, err := Connect(addr, c.CQLVersion, c.Timeout)
		if err != nil {
			<-time.After(delay)
			if delay *= 2; delay > c.DelayMax {
				delay = c.DelayMax
			}
			continue
		}
		c.pool.AddNode(conn)
		go func() {
			conn.Serve()
			c.pool.RemoveNode(conn)
			c.connect(addr)
		}()
		return
	}
}

func (c *Cluster) CreateSession() *Session {
	c.initOnce.Do(c.init)
	return NewSession(c.pool)
}
