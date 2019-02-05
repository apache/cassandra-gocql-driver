// Copyright 2019 Gocql Owners

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build all cassandra

package gocql

import (
	"sync/atomic"

	"testing"
)

func BenchmarkConnStress(b *testing.B) {
	const workers = 16

	cluster := createCluster()
	cluster.NumConns = 1
	session := createSessionFromCluster(cluster, b)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE IF NOT EXISTS conn_stress (id int primary key)"); err != nil {
		b.Fatal(err)
	}

	var seed uint64
	writer := func(pb *testing.PB) {
		seed := atomic.AddUint64(&seed, 1)
		var i uint64 = 0
		for pb.Next() {
			if err := session.Query("insert into conn_stress (id) values (?)", i*seed).Exec(); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	}

	b.SetParallelism(workers)
	b.RunParallel(writer)
}

func BenchmarkConnRoutingKey(b *testing.B) {
	const workers = 16

	cluster := createCluster()
	cluster.NumConns = 1
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	session := createSessionFromCluster(cluster, b)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE IF NOT EXISTS routing_key_stress (id int primary key)"); err != nil {
		b.Fatal(err)
	}

	var seed uint64
	writer := func(pb *testing.PB) {
		seed := atomic.AddUint64(&seed, 1)
		var i uint64 = 0
		query := session.Query("insert into routing_key_stress (id) values (?)")

		for pb.Next() {
			if _, err := query.Bind(i * seed).GetRoutingKey(); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	}

	b.SetParallelism(workers)
	b.RunParallel(writer)
}
