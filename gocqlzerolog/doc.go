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

// Package gocqlzerolog provides Zerolog logger integration for the gocql Cassandra driver.
//
// # Overview
//
// This package integrates the popular Zerolog structured logging library with gocql,
// allowing you to use Zerolog's zero-allocation logging features for database operations.
// It implements the gocql.StructuredLogger interface and converts gocql log fields
// to Zerolog fields automatically.
//
// # Basic Usage
//
// To use Zerolog logging with gocql, create a Zerolog logger and wrap it with NewZerologLogger:
//
//	import (
//		"os"
//		"github.com/rs/zerolog"
//		"github.com/apache/cassandra-gocql-driver/v2"
//		"github.com/apache/cassandra-gocql-driver/v2/gocqlzerolog"
//	)
//
//	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Logger = gocqlzerolog.NewZerologLogger(zerologLogger)
//
//	session, err := cluster.CreateSession()
//	if err != nil {
//		panic(err)
//	}
//	defer session.Close()
//
// # Named vs Unnamed Loggers
//
// The package provides two functions for creating logger instances:
//
// - NewZerologLogger: Creates a logger with a global context containing a "logger" field set to "gocql"
// - NewUnnamedZerologLogger: Creates a logger without modifying the context, allowing you to control naming
//
// Example with named logger:
//
//	// This will add a "logger": "gocql" field to all log entries
//	cluster.Logger = gocqlzerolog.NewZerologLogger(zerologLogger)
//
// Example with unnamed logger (custom naming):
//
//	// You control the logger context
//	customLogger := zerologLogger.With().Str("component", "cassandra-client").Logger()
//	cluster.Logger = gocqlzerolog.NewUnnamedZerologLogger(customLogger)
//
// # Field Type Conversion
//
// The package automatically converts gocql log fields to appropriate Zerolog field types:
//
// - Boolean fields → zerolog.Event.Bool
// - Integer fields → zerolog.Event.Int64
// - String fields → zerolog.Event.Str
// - Other types → zerolog.Event.Any
//
// # Log Levels
//
// The gocql log levels are mapped to Zerolog log levels as follows:
//
// - gocql Error → zerolog.Logger.Error()
// - gocql Warning → zerolog.Logger.Warn()
// - gocql Info → zerolog.Logger.Info()
// - gocql Debug → zerolog.Logger.Debug()
//
// # Configuration Examples
//
// # Recommended: Simple Setup
//
// For most use cases, create a basic zerolog logger:
//
//	import (
//		"os"
//		"github.com/rs/zerolog"
//		"github.com/apache/cassandra-gocql-driver/v2"
//		"github.com/apache/cassandra-gocql-driver/v2/gocqlzerolog"
//	)
//
//	// Basic structured logging (JSON output)
//	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Logger = gocqlzerolog.NewZerologLogger(zerologLogger)
//
//	// Human-readable console output for development
//	zerologLogger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
//	cluster.Logger = gocqlzerolog.NewZerologLogger(zerologLogger)
//
// # Advanced Configuration
//
// For advanced zerolog configuration options (sampling, custom outputs, global settings, etc.),
// refer to the official Zerolog documentation: https://github.com/rs/zerolog
//
// Once you have configured your Zerolog logger, simply wrap it with gocqlzerolog:
//
//	zerologLogger := // ... your custom Zerolog logger configuration
//	cluster.Logger = gocqlzerolog.NewZerologLogger(zerologLogger)
//
// # Performance Considerations
//
// This integration is designed to be high-performance and zero-allocation:
//
// - Uses Zerolog's zero-allocation logging capabilities
// - Minimizes memory allocations through efficient field conversion
// - Leverages Zerolog's optimized structured logging
//
// # Thread Safety
//
// The logger implementation is thread-safe and can be used concurrently
// across multiple goroutines, as guaranteed by the underlying Zerolog logger.
package gocqlzerolog // import "github.com/apache/cassandra-gocql-driver/v2/gocqlzerolog"
