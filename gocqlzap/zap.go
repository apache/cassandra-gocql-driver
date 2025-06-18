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

package gocqlzap

import (
	"go.uber.org/zap"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const DefaultName = "gocql"

type Logger interface {
	gocql.StructuredLogger
	ZapLogger() *zap.Logger
}

type logger struct {
	zapLogger *zap.Logger
}

// NewZapLogger creates a new zap based logger with the logger name set to DefaultName
func NewZapLogger(l *zap.Logger) Logger {
	return &logger{zapLogger: l.Named(DefaultName)}
}

// NewUnnamedZapLogger doesn't set the logger name so the user can set the name of the logger
// before providing it to this function (or just leave it unset)
func NewUnnamedZapLogger(l *zap.Logger) Logger {
	return &logger{zapLogger: l}
}

func (rec *logger) ZapLogger() *zap.Logger {
	return rec.zapLogger
}

func (rec *logger) log(fields []gocql.LogField) *zap.Logger {
	childLogger := rec.zapLogger
	for _, field := range fields {
		childLogger = childLogger.WithLazy(zapField(field))
	}
	return childLogger
}

func zapField(field gocql.LogField) zap.Field {
	switch field.Value.LogFieldValueType() {
	case gocql.LogFieldTypeBool:
		return zap.Bool(field.Name, field.Value.Bool())
	case gocql.LogFieldTypeInt64:
		return zap.Int64(field.Name, field.Value.Int64())
	case gocql.LogFieldTypeString:
		return zap.String(field.Name, field.Value.String())
	default:
		return zap.Any(field.Name, field.Value.Any())
	}
}

func (rec *logger) Error(msg string, fields ...gocql.LogField) {
	rec.log(fields).Error(msg)
}

func (rec *logger) Warning(msg string, fields ...gocql.LogField) {
	rec.log(fields).Warn(msg)
}

func (rec *logger) Info(msg string, fields ...gocql.LogField) {
	rec.log(fields).Info(msg)
}

func (rec *logger) Debug(msg string, fields ...gocql.LogField) {
	rec.log(fields).Debug(msg)
}
