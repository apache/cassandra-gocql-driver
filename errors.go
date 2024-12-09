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

package gocql

import (
	"github.com/gocql/gocql/internal"
)

// See CQL Binary Protocol v5, section 8 for more details.
// https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec
const (
	// ErrCodeServer indicates unexpected error on server-side.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1246-L1247
	// Deprecated: use gocql_errors.ErrCodeServer instead.
	ErrCodeServer = internal.ErrCodeServer
	// ErrCodeProtocol indicates a protocol violation by some client message.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1248-L1250
	// Deprecated: use gocql_errors.ErrCodeProtocol instead.
	ErrCodeProtocol = internal.ErrCodeProtocol
	// ErrCodeCredentials indicates missing required authentication.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1251-L1254
	// Deprecated: use gocql_errors.ErrCodeCredentials instead.
	ErrCodeCredentials = internal.ErrCodeCredentials
	// ErrCodeUnavailable indicates unavailable error.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1255-L1265
	// Deprecated: use gocql_errors.ErrCodeUnavailable instead.
	ErrCodeUnavailable = internal.ErrCodeUnavailable
	// ErrCodeOverloaded returned in case of request on overloaded node coordinator.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1266-L1267
	// Deprecated: use gocql_errors.ErrCodeOverloaded instead.
	ErrCodeOverloaded = internal.ErrCodeOverloaded
	// ErrCodeBootstrapping returned from the coordinator node in bootstrapping phase.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1268-L1269
	// Deprecated: use gocql_errors.ErrCodeBootstrapping instead.
	ErrCodeBootstrapping = internal.ErrCodeBootstrapping
	// ErrCodeTruncate indicates truncation exception.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1270
	// Deprecated: use gocql_errors.ErrCodeTruncate instead.
	ErrCodeTruncate = internal.ErrCodeTruncate
	// ErrCodeWriteTimeout returned in case of timeout during the request write.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1271-L1304
	// Deprecated: use gocql_errors.ErrCodeWriteTimeout instead.
	ErrCodeWriteTimeout = internal.ErrCodeWriteTimeout
	// ErrCodeReadTimeout returned in case of timeout during the request read.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1305-L1321
	// Deprecated: use gocql_errors.ErrCodeReadTimeout instead.
	ErrCodeReadTimeout = internal.ErrCodeReadTimeout
	// ErrCodeReadFailure indicates request read error which is not covered by ErrCodeReadTimeout.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1322-L1340
	// Deprecated: use gocql_errors.ErrCodeReadFailure instead.
	ErrCodeReadFailure = internal.ErrCodeReadFailure
	// ErrCodeFunctionFailure indicates an error in user-defined function.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1341-L1347
	// Deprecated: use gocql_errors.ErrCodeFunctionFailure instead.
	ErrCodeFunctionFailure = internal.ErrCodeFunctionFailure
	// ErrCodeWriteFailure indicates request write error which is not covered by ErrCodeWriteTimeout.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1348-L1385
	// Deprecated: use gocql_errors.ErrCodeWriteFailure instead.
	ErrCodeWriteFailure = internal.ErrCodeWriteFailure
	// ErrCodeCDCWriteFailure is defined, but not yet documented in CQLv5 protocol.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1386
	// Deprecated: use gocql_errors.ErrCodeCDCWriteFailure instead.
	ErrCodeCDCWriteFailure = internal.ErrCodeCDCWriteFailure
	// ErrCodeCASWriteUnknown indicates only partially completed CAS operation.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1387-L1397
	// Deprecated: use gocql_errors.ErrCodeCASWriteUnknown instead.
	ErrCodeCASWriteUnknown = internal.ErrCodeCASWriteUnknown
	// ErrCodeSyntax indicates the syntax error in the query.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1399
	// Deprecated: use gocql_errors.ErrCodeSyntax instead.
	ErrCodeSyntax = internal.ErrCodeSyntax
	// ErrCodeUnauthorized indicates access rights violation by user on performed operation.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1400-L1401
	// Deprecated: use gocql_errors.ErrCodeUnauthorized instead.
	ErrCodeUnauthorized = internal.ErrCodeUnauthorized
	// ErrCodeInvalid indicates invalid query error which is not covered by ErrCodeSyntax.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1402
	// Deprecated: use gocql_errors.ErrCodeInvalid instead.
	ErrCodeInvalid = internal.ErrCodeInvalid
	// ErrCodeConfig indicates the configuration error.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1403
	// Deprecated: use gocql_errors.ErrCodeConfig instead.
	ErrCodeConfig = internal.ErrCodeConfig
	// ErrCodeAlreadyExists is returned for the requests creating the existing keyspace/table.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1404-L1413
	// Deprecated: use gocql_errors.ErrCodeAlreadyExists instead.
	ErrCodeAlreadyExists = internal.ErrCodeAlreadyExists
	// ErrCodeUnprepared returned from the host for prepared statement which is unknown.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1414-L1417
	// Deprecated: use gocql_errors.ErrCodeUnprepared instead.
	ErrCodeUnprepared = internal.ErrCodeUnprepared
)

type RequestError = internal.RequestError

type errorFrame = internal.ErrorFrame

type RequestErrUnavailable = internal.RequestErrUnavailable

type ErrorMap = internal.ErrorMap

type RequestErrWriteTimeout = internal.RequestErrWriteTimeout

type RequestErrWriteFailure = internal.RequestErrWriteFailure

type RequestErrCDCWriteFailure = internal.RequestErrCDCWriteFailure

type RequestErrReadTimeout = internal.RequestErrReadTimeout

type RequestErrAlreadyExists = internal.RequestErrAlreadyExists

type RequestErrUnprepared = internal.RequestErrUnprepared

type RequestErrReadFailure = internal.RequestErrReadFailure

type RequestErrFunctionFailure = internal.RequestErrFunctionFailure

type RequestErrCASWriteUnknown = internal.RequestErrCASWriteUnknown
