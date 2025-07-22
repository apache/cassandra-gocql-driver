/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package hostpool provides host selection policies for gocql that integrate
// with the go-hostpool library for intelligent host pooling and load balancing.
//
// # Overview
//
// This package allows gocql to use go-hostpool's intelligent host selection
// algorithms, including round-robin and epsilon greedy policies that can
// automatically avoid problematic hosts and adapt to host performance.
//
// # Basic Usage
//
// To use host pool policies with gocql, create a host pool and set it as your
// cluster's host selection policy:
//
//	import (
//		"github.com/hailocab/go-hostpool"
//		"github.com/apache/cassandra-gocql-driver/v2"
//		"github.com/apache/cassandra-gocql-driver/v2/hostpool"
//	)
//
//	// Create an epsilon greedy pool for adaptive load balancing
//	pool := hostpool.NewEpsilonGreedy(
//		nil, // Host list populated automatically by gocql
//		0,   // Use default 5-minute decay duration
//		&hostpool.LinearEpsilonValueCalculator{}, // Example calculator
//	)
//
//	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
//	cluster.PoolConfig.HostSelectionPolicy = hostpool.HostPoolHostPolicy(pool)
//
//	session, err := cluster.CreateSession()
//	if err != nil {
//		panic(err)
//	}
//	defer session.Close()
//
// # Host Pool Types
//
// # Simple Round Robin
//
// Basic round-robin selection suitable for testing and simple deployments:
//
//	pool := hostpool.New(nil) // Hosts populated by gocql
//	cluster.PoolConfig.HostSelectionPolicy = hostpool.HostPoolHostPolicy(pool)
//
// # Epsilon Greedy
//
// Adaptive selection that learns host performance and routes traffic accordingly:
//
//	// Example using LinearEpsilonValueCalculator
//	pool := hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{})
//	cluster.PoolConfig.HostSelectionPolicy = hostpool.HostPoolHostPolicy(pool)
//
//	// Other epsilon value calculators are also available:
//	// - LogEpsilonValueCalculator: Uses logarithmic scaling
//	// - PolynomialEpsilonValueCalculator: Uses polynomial scaling
//
// The epsilon greedy algorithm automatically:
// - Routes more traffic to faster-responding hosts
// - Reduces load on slower or problematic hosts
// - Adapts to changing host performance over time
// - Provides automatic failure avoidance
//
// # Integration Details
//
// The hostpool policy integrates seamlessly with gocql's host management:
//
// - Host list is automatically populated and updated by gocql
// - Host failures are automatically reported to the pool
// - The pool tracks response times and host performance
// - Works with gocql's reconnection and discovery mechanisms
//
// # Configuration Options
//
// For epsilon greedy pools, you can customize:
//
// - Decay duration: How long to average response times (default 5 minutes)
// - Value calculator: Algorithm for scoring hosts based on performance
//
// Choose the epsilon value calculator that best fits your performance characteristics
// and load balancing requirements. See the go-hostpool documentation for detailed
// configuration options and calculator behavior.
package hostpool
