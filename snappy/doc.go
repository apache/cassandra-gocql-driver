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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

// Package snappy provides Snappy compression for the Cassandra Native Protocol.
//
// Snappy compresses Native Protocol frame payloads as defined in the Cassandra
// Native Protocol specification. The protocol supports both compressed and
// uncompressed frame formats, with compression applied to frame payloads
// containing CQL envelopes.
//
// # Basic Usage
//
// To enable Snappy compression:
//
//	import (
//		"github.com/apache/cassandra-gocql-driver/v2"
//		"github.com/apache/cassandra-gocql-driver/v2/snappy"
//	)
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Compressor = &snappy.SnappyCompressor{}
//
// # Native Protocol Compression
//
// According to the Cassandra Native Protocol specification, compression operates
// on frame payloads containing streams of CQL envelopes. Each frame payload is
// compressed independently with no compression context between frames.
//
// # Protocol and Cassandra Version Support
//
// Snappy compression support varies by Cassandra version due to Native Protocol
// changes:
//
//   - Protocol v2 (Cassandra 2.0.x): LZ4 and Snappy supported
//   - Protocol v3 (Cassandra 2.1.x): LZ4 and Snappy supported
//   - Protocol v4 (Cassandra 2.2.x, 3.0.x, 3.x): LZ4 and Snappy supported
//   - Protocol v5 (Cassandra 4.0+): Only LZ4 supported (Snappy removed)
//
// Snappy is supported from Cassandra 2.0.x through 3.x, but is not available
// in Cassandra 4.0+. For applications targeting Cassandra 4.0+, use LZ4 compression.
//
// # Compatibility Notes
//
// When connecting to Cassandra 4.0+ clusters, Snappy compression will not be
// available as the protocol only supports LZ4. Applications should use LZ4
// for maximum compatibility or implement version-specific compression selection.
//
// # Performance Characteristics
//
// LZ4 is generally recommended as the default choice for most applications.
// For performance optimization, benchmark your specific workload with both
// compression algorithms, though Snappy is mainly useful if benchmarking
// shows it performs better for your specific use case.
package snappy // import "github.com/apache/cassandra-gocql-driver/v2/snappy"
