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

// Package gocqlzap provides Zap logger integration for the gocql Cassandra driver.
//
// # Overview
//
// This package integrates the popular Zap structured logging library with gocql,
// allowing you to use Zap's high-performance logging features for database operations.
// It implements the gocql.StructuredLogger interface and converts gocql log fields
// to Zap fields automatically.
//
// # Basic Usage
//
// To use Zap logging with gocql, create a Zap logger and wrap it with NewZapLogger:
//
//	import (
//		"go.uber.org/zap"
//		"github.com/apache/cassandra-gocql-driver/v2"
//		"github.com/apache/cassandra-gocql-driver/v2/gocqlzap"
//	)
//
//	zapLogger, _ := zap.NewProduction()
//	defer zapLogger.Sync()
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Logger = gocqlzap.NewZapLogger(zapLogger)
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
// - NewZapLogger: Creates a logger with a default name "gocql" using Zap's Named() method
// - NewUnnamedZapLogger: Creates a logger without setting a name, allowing you to control naming
//
// Example with named logger:
//
//	// This will add a "logger" field with value "gocql" to all log entries
//	cluster.Logger = gocqlzap.NewZapLogger(zapLogger)
//
// Example with unnamed logger (custom naming):
//
//	// You control the logger name
//	customLogger := zapLogger.Named("my-cassandra-client")
//	cluster.Logger = gocqlzap.NewUnnamedZapLogger(customLogger)
//
// # Field Type Conversion
//
// The package automatically converts gocql log fields to appropriate Zap field types:
//
// - Boolean fields → zap.Bool
// - Integer fields → zap.Int64
// - String fields → zap.String
// - Other types → zap.Any
//
// # Log Levels
//
// The gocql log levels are mapped to Zap log levels as follows:
//
// - gocql Error → zap.Error
// - gocql Warning → zap.Warn
// - gocql Info → zap.Info
// - gocql Debug → zap.Debug
//
// # Configuration Examples
//
// # Recommended: Use Built-in Configurations
//
// For most use cases, use Zap's built-in configurations which provide sensible defaults:
//
//	// For production
//	zapLogger, _ := zap.NewProduction()
//	defer zapLogger.Sync()
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Logger = gocqlzap.NewZapLogger(zapLogger)
//
//	// For development (includes caller info, console encoding)
//	zapLogger, _ := zap.NewDevelopment()
//	defer zapLogger.Sync()
//
//	cluster := gocql.NewCluster("127.0.0.1")
//	cluster.Logger = gocqlzap.NewZapLogger(zapLogger)
//
// # Custom Configuration
//
// For advanced configuration options, refer to the official Zap documentation:
// https://pkg.go.dev/go.uber.org/zap
//
// Once you have configured your Zap logger, simply wrap it with gocqlzap:
//
//	zapLogger := // ... your custom Zap logger configuration
//	cluster.Logger = gocqlzap.NewZapLogger(zapLogger)
//
// # Performance Considerations
//
// This integration is designed to be high-performance:
//
// - Uses Zap's WithLazy() for efficient field construction
// - Minimizes allocations by reusing field conversion logic
// - Leverages Zap's optimized structured logging capabilities
//
// # Thread Safety
//
// The logger implementation is thread-safe and can be used concurrently
// across multiple goroutines, as guaranteed by the underlying Zap logger.
package gocqlzap // import "github.com/apache/cassandra-gocql-driver/v2/gocqlzap"
