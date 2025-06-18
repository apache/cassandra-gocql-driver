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

package gocqlzerolog

import (
	"github.com/rs/zerolog"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const DefaultName = "gocql"
const DefaultNameField = "logger"

type Logger interface {
	gocql.StructuredLogger
	ZerologLogger() zerolog.Logger
}

type logger struct {
	zerologLogger zerolog.Logger
}

// NewZerologLogger creates a new zerolog based logger with a global context containing a field
// with name "logger" and value "gocql", i.e.:
//
//	l.With().Str("logger", "gocql").Logger()
func NewZerologLogger(l zerolog.Logger) Logger {
	return &logger{zerologLogger: l.With().Str(DefaultNameField, DefaultName).Logger()}
}

// NewUnnamedZerologLogger creates a new zerolog based logger without modifying its context like
// NewZerologLogger does.
func NewUnnamedZerologLogger(l zerolog.Logger) Logger {
	return &logger{zerologLogger: l}
}

func (rec *logger) ZerologLogger() zerolog.Logger {
	return rec.zerologLogger
}

func (rec *logger) log(event *zerolog.Event, fields ...gocql.LogField) *zerolog.Event {
	for _, field := range fields {
		event = zerologEvent(event, field)
	}
	return event
}

func zerologEvent(event *zerolog.Event, field gocql.LogField) *zerolog.Event {
	switch field.Value.LogFieldValueType() {
	case gocql.LogFieldTypeBool:
		return event.Bool(field.Name, field.Value.Bool())
	case gocql.LogFieldTypeInt64:
		return event.Int64(field.Name, field.Value.Int64())
	case gocql.LogFieldTypeString:
		return event.Str(field.Name, field.Value.String())
	default:
		return event.Any(field.Name, field.Value.Any())
	}
}

func (rec *logger) Error(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Error(), fields...).Msg(msg)
}

func (rec *logger) Warning(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Warn(), fields...).Msg(msg)
}

func (rec *logger) Info(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Info(), fields...).Msg(msg)
}

func (rec *logger) Debug(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Debug(), fields...).Msg(msg)
}
