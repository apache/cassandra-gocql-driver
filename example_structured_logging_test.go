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

package gocql_test

import (
	"context"
	"log"
	"os"

	"github.com/rs/zerolog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/gocqlzap"
	"github.com/apache/cassandra-gocql-driver/v2/gocqlzerolog"
)

// Example_structuredLogging demonstrates the new structured logging features
// introduced in 2.0.0. The driver now supports structured logging with proper
// log levels and integration with popular logging libraries like Zap and Zerolog.
// This example shows production-ready configurations for structured logging with
// proper component separation to distinguish between application and driver logs.
func Example_structuredLogging() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.log_demo(id int, value text, PRIMARY KEY(id));
	*/

	ctx := context.Background()

	// Example 1: Using Zap logger integration
	// Create a production Zap logger with structured JSON output and human-readable timestamps
	// Production config uses JSON encoding, info level, and proper error handling
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // Human-readable timestamp format

	zapLogger, err := config.Build()
	if err != nil {
		log.Fatal(err)
	}
	defer zapLogger.Sync()

	// Create base logger with service identifier
	baseLogger := zapLogger.With(zap.String("service", "gocql-app"))

	// Create application and driver loggers with component identifiers
	appLogger := baseLogger.With(zap.String("component", "app"))
	driverLogger := baseLogger.With(zap.String("component", "gocql-driver"))

	appLogger.Info("Starting Zap structured logging example",
		zap.String("example", "structured_logging"),
		zap.String("logger_type", "zap"))

	// Create gocql logger from driver logger
	gocqlZapLogger := gocqlzap.NewUnnamedZapLogger(driverLogger)

	zapCluster := gocql.NewCluster("localhost:9042")
	zapCluster.Keyspace = "example"
	zapCluster.Logger = gocqlZapLogger

	zapSession, err := zapCluster.CreateSession()
	if err != nil {
		appLogger.Fatal("Failed to create session", zap.Error(err))
	}
	defer zapSession.Close()

	// Perform some operations that will generate logs
	appLogger.Info("Inserting data into database",
		zap.String("operation", "insert"),
		zap.Int("record_id", 1))

	err = zapSession.Query("INSERT INTO example.log_demo (id, value) VALUES (?, ?)").
		Bind(1, "zap logging demo").
		ExecContext(ctx)
	if err != nil {
		appLogger.Error("Insert operation failed", zap.Error(err))
		log.Fatal(err)
	}

	appLogger.Info("Querying data from database",
		zap.String("operation", "select"),
		zap.Int("record_id", 1))

	var id int
	var value string
	iter := zapSession.Query("SELECT id, value FROM example.log_demo WHERE id = ?").
		Bind(1).
		IterContext(ctx)

	if iter.Scan(&id, &value) {
		// Successfully scanned the row
	}
	err = iter.Close()
	if err != nil {
		appLogger.Error("Select operation failed", zap.Error(err))
		log.Fatal(err)
	}

	appLogger.Info("Database operation completed successfully",
		zap.String("operation", "select"),
		zap.Int("record_id", id),
		zap.String("record_value", value))

	// Example 2: Using Zerolog integration
	// Create a production Zerolog logger with structured JSON output
	// Production config includes timestamps, service info, and appropriate log level
	baseZerologLogger := zerolog.New(os.Stdout).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Str("service", "gocql-app").
		Logger()

	// Create application logger with component identifier
	appZerologLogger := baseZerologLogger.With().
		Str("component", "app").
		Logger()

	// Create driver logger with component identifier
	driverZerologLogger := baseZerologLogger.With().
		Str("component", "gocql-driver").
		Logger()

	appZerologLogger.Info().
		Str("example", "structured_logging").
		Str("logger_type", "zerolog").
		Msg("Starting Zerolog structured logging example")

	// Create gocql logger from driver logger
	gocqlZerologLogger := gocqlzerolog.NewUnnamedZerologLogger(driverZerologLogger)

	zerologCluster := gocql.NewCluster("localhost:9042")
	zerologCluster.Keyspace = "example"
	zerologCluster.Logger = gocqlZerologLogger

	zerologSession, err := zerologCluster.CreateSession()
	if err != nil {
		appZerologLogger.Fatal().Err(err).Msg("Failed to create session")
	}
	defer zerologSession.Close()

	// Perform operations with Zerolog
	appZerologLogger.Info().
		Str("operation", "insert").
		Int("record_id", 2).
		Msg("Inserting data into database")

	err = zerologSession.Query("INSERT INTO example.log_demo (id, value) VALUES (?, ?)").
		Bind(2, "zerolog logging demo").
		ExecContext(ctx)
	if err != nil {
		appZerologLogger.Error().Err(err).Msg("Insert operation failed")
		log.Fatal(err)
	}

	appZerologLogger.Info().
		Str("operation", "select").
		Int("record_id", 2).
		Msg("Querying data from database")

	iter = zerologSession.Query("SELECT id, value FROM example.log_demo WHERE id = ?").
		Bind(2).
		IterContext(ctx)

	if iter.Scan(&id, &value) {
		// Successfully scanned the row
	}
	err = iter.Close()
	if err != nil {
		appZerologLogger.Error().Err(err).Msg("Select operation failed")
		log.Fatal(err)
	}

	appZerologLogger.Info().
		Str("operation", "select").
		Int("record_id", id).
		Str("record_value", value).
		Msg("Database operation completed successfully")

	// Example 1 - Zap structured logging output (JSON format):
	// {"level":"info","timestamp":"2023-12-31T12:00:00.000Z","msg":"Starting Zap structured logging example","service":"gocql-app","component":"app","example":"structured_logging","logger_type":"zap"}
	// {"level":"info","timestamp":"2023-12-31T12:00:00.100Z","msg":"Discovered protocol version.","service":"gocql-app","component":"gocql-driver","protocol_version":5}
	// {"level":"info","timestamp":"2023-12-31T12:00:00.200Z","msg":"Control connection connected to host.","service":"gocql-app","component":"gocql-driver","host_addr":"127.0.0.1","host_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890"}
	// {"level":"info","timestamp":"2023-12-31T12:00:00.300Z","msg":"Refreshed ring.","service":"gocql-app","component":"gocql-driver","ring":"[127.0.0.1-a1b2c3d4-e5f6-7890-abcd-ef1234567890:UP]"}
	// {"level":"info","timestamp":"2023-12-31T12:00:00.400Z","msg":"Session initialized successfully.","service":"gocql-app","component":"gocql-driver"}
	// {"level":"info","timestamp":"2023-12-31T12:00:01.000Z","msg":"Inserting data into database","service":"gocql-app","component":"app","operation":"insert","record_id":1}
	// {"level":"info","timestamp":"2023-12-31T12:00:02.000Z","msg":"Querying data from database","service":"gocql-app","component":"app","operation":"select","record_id":1}
	// {"level":"info","timestamp":"2023-12-31T12:00:03.000Z","msg":"Database operation completed successfully","service":"gocql-app","component":"app","operation":"select","record_id":1,"record_value":"zap logging demo"}
	//
	// Example 2 - Zerolog structured logging output (JSON format):
	// {"level":"info","service":"gocql-app","component":"app","example":"structured_logging","logger_type":"zerolog","time":"2023-12-31T12:00:10Z","message":"Starting Zerolog structured logging example"}
	// {"level":"info","service":"gocql-app","component":"gocql-driver","protocol_version":5,"time":"2023-12-31T12:00:10.1Z","message":"Discovered protocol version."}
	// {"level":"info","service":"gocql-app","component":"gocql-driver","host_addr":"127.0.0.1","host_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","time":"2023-12-31T12:00:10.2Z","message":"Control connection connected to host."}
	// {"level":"info","service":"gocql-app","component":"gocql-driver","ring":"[127.0.0.1-a1b2c3d4-e5f6-7890-abcd-ef1234567890:UP]","time":"2023-12-31T12:00:10.3Z","message":"Refreshed ring."}
	// {"level":"info","service":"gocql-app","component":"gocql-driver","time":"2023-12-31T12:00:10.4Z","message":"Session initialized successfully."}
	// {"level":"info","service":"gocql-app","component":"app","operation":"insert","record_id":2,"time":"2023-12-31T12:00:11Z","message":"Inserting data into database"}
	// {"level":"info","service":"gocql-app","component":"app","operation":"select","record_id":2,"time":"2023-12-31T12:00:12Z","message":"Querying data from database"}
	// {"level":"info","service":"gocql-app","component":"app","operation":"select","record_id":2,"record_value":"zerolog logging demo","time":"2023-12-31T12:00:13Z","message":"Database operation completed successfully"}
}
