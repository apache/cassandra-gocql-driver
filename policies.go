// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"sync/atomic"
)

//RetyPolicy policy defines the rety stradegy for a query. It can be assigned to the
//cluster configuration or per query. Set a property to 0 for no retries to happen at
//that level. Setting all properties to 0 will prevent a query from being retried up
// error.
type RetryPolicy struct {
	Rack       int
	DataCenter int
	Count      int
}

type LoadBalancePolicy interface {
	Pick(hostIDs []UUID, hosts map[UUID]Host) *Conn
}

type RoundRobin struct {
	pos uint64
}

func (r *RoundRobin) Pick(hostIDs []UUID, hosts map[UUID]Host) *Conn {
	h := uint64(len(hostIDs))
	if h > 0 {
		pos := atomic.AddUint64(&r.pos, 1)
		host := hosts[hostIDs[pos%h]]
		conns := len(host.conn)
		for i := 0; i < conns; i++ {
			if len(host.conn[i].uniq) > 0 {
				return host.conn[i]
			}
		}
	}
	return nil
}
