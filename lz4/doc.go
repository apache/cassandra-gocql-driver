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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

// Package lz4 provides LZ4 compression for the Cassandra Native Protocol.
//
// LZ4 compresses Native Protocol frame payloads as defined in the Cassandra
// Native Protocol specification. The protocol supports both compressed and
// uncompressed frame formats, with compression applied to frame payloads
// containing CQL envelopes.
//
// # Basic Usage
//
// To enable LZ4 compression:
//
//	import (
//		"github.com/apache/cassandra-gocql-driver/v2"
//		"github.com/apache/cassandra-gocql-driver/v2/lz4"
//	)
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Compressor = &lz4.LZ4Compressor{}
//
// # Native Protocol Compression
//
// According to the Cassandra Native Protocol specification, compression operates
// on frame payloads containing streams of CQL envelopes. Each frame payload is
// compressed independently with no compression context between frames.
//
// # Protocol and Cassandra Version Support
//
// LZ4 compression is supported across all Native Protocol versions that support
// compression, with corresponding Cassandra version support:
//
//   - Protocol v2 (Cassandra 2.0.x): LZ4 and Snappy supported
//   - Protocol v3 (Cassandra 2.1.x): LZ4 and Snappy supported
//   - Protocol v4 (Cassandra 2.2.x, 3.0.x, 3.x): LZ4 and Snappy supported
//   - Protocol v5 (Cassandra 4.0+): Only LZ4 supported (Snappy removed)
//
// LZ4 is supported from Cassandra 2.0+ through current versions. In Cassandra 4.0+,
// LZ4 became the only supported compression algorithm.
//
// # Performance Characteristics
//
// LZ4 generally provides a good balance of compression speed and compression ratio,
// making it a solid default choice for most applications. The effectiveness of
// compression depends on the specific CQL query patterns, result set sizes, and
// frame payload characteristics in your application.
//
// For optimal performance, benchmark both LZ4 and Snappy with your specific
// workload, though LZ4 is typically a good starting point.
package lz4 // import "github.com/apache/cassandra-gocql-driver/v2/lz4"
