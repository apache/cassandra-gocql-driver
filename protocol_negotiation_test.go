//go:build all || unit
// +build all unit

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

package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type requestHandlerForProtocolNegotiationTest struct {
	supportedProtocolVersions []protoVersion
	supportedBetaProtocols    []protoVersion

	// forces stream id to 0
	forceZeroStreamID bool

	forceCloseConnection bool
}

func (r *requestHandlerForProtocolNegotiationTest) supportsBetaProtocol(version protoVersion) bool {
	return slices.Contains(r.supportedBetaProtocols, version)
}

func (r *requestHandlerForProtocolNegotiationTest) supportsProtocol(version protoVersion) bool {
	return slices.Contains(r.supportedProtocolVersions, version)
}

func (r *requestHandlerForProtocolNegotiationTest) hasBetaFlag(header *frameHeader) bool {
	return header.flags&flagBetaProtocol == flagBetaProtocol
}

func (r *requestHandlerForProtocolNegotiationTest) createBetaFlagUnsetProtocolErrorMessage(version protoVersion) string {
	return fmt.Sprintf("Beta version of the protocol used (%d/v%d-beta), but USE_BETA flag is unset", version, version)
}

func (r *requestHandlerForProtocolNegotiationTest) handle(_ *TestServer, reqFrame, respFrame *framer) error {
	if r.forceCloseConnection {
		return fmt.Errorf("NEGOTIATION TEST: forcing close connection")
	}

	stream := reqFrame.header.stream

	// If a client uses beta protocol, but the USE_BETA flag is not set, we respond with an error
	if r.supportsBetaProtocol(reqFrame.header.version) && !r.hasBetaFlag(reqFrame.header) {
		if r.forceZeroStreamID {
			stream = 0
		}
		respFrame.writeHeader(0, opError, stream)
		respFrame.writeInt(ErrCodeProtocol)
		respFrame.writeString(r.createBetaFlagUnsetProtocolErrorMessage(reqFrame.header.version))
		return nil
	}

	// if a client uses an unsupported protocol version, we respond with an error
	if !r.supportsProtocol(reqFrame.header.version) {
		if r.forceZeroStreamID {
			stream = 0
		}
		respFrame.writeHeader(0, opError, stream)
		respFrame.writeInt(ErrCodeProtocol)
		respFrame.writeString(fmt.Sprintf("NEGOTIATION TEST: Unsupported protocol version %d", reqFrame.header.version))
		return nil
	}

	switch reqFrame.header.op {
	case opStartup, opRegister:
		respFrame.writeHeader(0, opReady, stream)
	case opOptions:
		// Emulating C* behavior.
		// If a client uses an unsupported protocol version, C* responds with supported versions to 0 stream id.
		// If a client uses a beta protocol version, but the USE_BETA flag is not set, C* responds with supported versions to 0 stream id.
		if r.forceZeroStreamID && !(r.supportsProtocol(reqFrame.header.version) || r.supportsBetaProtocol(reqFrame.header.version) && !r.hasBetaFlag(reqFrame.header)) {
			stream = 0
		}
		respFrame.writeHeader(0, opSupported, stream)
		var supportedVersionsWithDesc []string
		for _, supportedVersion := range r.supportedProtocolVersions {
			supportedVersionsWithDesc = append(supportedVersionsWithDesc, fmt.Sprintf("%d/v%d", supportedVersion, supportedVersion))
		}
		for _, betaProtocol := range r.supportedBetaProtocols {
			supportedVersionsWithDesc = append(supportedVersionsWithDesc, fmt.Sprintf("%d/v%d-beta", betaProtocol, betaProtocol))
		}
		supported := map[string][]string{
			"PROTOCOL_VERSIONS": supportedVersionsWithDesc,
		}
		respFrame.writeStringMultiMap(supported)
	case opQuery:
		respFrame.writeHeader(0, opResult, stream)
		respFrame.writeInt(resultKindRows)
		respFrame.writeInt(int32(flagGlobalTableSpec))
		respFrame.writeInt(1)
		respFrame.writeString("system")
		respFrame.writeString("local")
		respFrame.writeString("rack")
		respFrame.writeShort(uint16(TypeVarchar))
		respFrame.writeInt(1)
		respFrame.writeInt(int32(len("rack-1")))
		respFrame.writeString("rack-1")
	case opPrepare:
		// This doesn't really make any sense, but it's enough to test the protocol negotiation
		respFrame.writeHeader(0, opResult, stream)
		respFrame.writeInt(resultKindPrepared)
		// <id>
		respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 111))
		if respFrame.proto >= protoVersion5 {
			respFrame.writeShortBytes(binary.BigEndian.AppendUint64(nil, 222))
		}
		// <metadata>
		respFrame.writeInt(0) // <flags>
		respFrame.writeInt(0) // <columns_count>
		if reqFrame.header.version >= protoVersion4 {
			respFrame.writeInt(0) // <pk_count>
		}
		// <result_metadata>
		respFrame.writeInt(int32(flagGlobalTableSpec)) // <flags>
		respFrame.writeInt(1)                          // <columns_count>
		// <global_table_spec>
		respFrame.writeString("system")
		respFrame.writeString("keyspaces")
		// <col_spec_0>
		respFrame.writeString("col0")             // <name>
		respFrame.writeShort(uint16(TypeBoolean)) // <type>
	case opExecute:
		// This doesn't really make any sense, but it's enough to test the protocol negotiation
		respFrame.writeHeader(0, opResult, stream)
		respFrame.writeInt(resultKindRows)
		// <metadata>
		respFrame.writeInt(0) // <flags>
		respFrame.writeInt(0) // <columns_count>
		// <rows_count>
		respFrame.writeInt(0)
	}

	return nil
}

func mockedErrorCodeHandler(errorCode int) func(*TestServer, *framer, *framer) error {
	return func(_ *TestServer, reqFrame *framer, respFrame *framer) error {
		reqFrame.writeHeader(0, opError, reqFrame.header.stream)
		reqFrame.writeInt(int32(errorCode))
		reqFrame.writeString(fmt.Sprintf("NEGOTIATION TEST: Error code %d", errorCode))
		return nil
	}
}

func TestProtocolNegotiation(t *testing.T) {
	testCases := []struct {
		name                  string
		supportedVersions     []protoVersion
		supportedBetaVersions []protoVersion
		expectedVersion       protoVersion
		expectedErrorMsg      string

		forceZeroStreamID bool
		overrideHost      string

		requestHandler func(*TestServer, *framer, *framer) error
	}{
		{
			name:              "all supported versions",
			supportedVersions: []protoVersion{protoVersion3, protoVersion4, protoVersion5},
			expectedVersion:   protoVersion5,
		},
		{
			name:                  "v5-beta is supported",
			supportedVersions:     []protoVersion{protoVersion3, protoVersion4},
			supportedBetaVersions: []protoVersion{protoVersion5},
			expectedVersion:       protoVersion4,
		},
		{
			name:              "v5 is unsupported",
			supportedVersions: []protoVersion{protoVersion3, protoVersion4},
			expectedVersion:   protoVersion4,
		},
		{
			name:              "all supported versions / 0 stream id",
			supportedVersions: []protoVersion{protoVersion3, protoVersion4, protoVersion5},
			expectedVersion:   protoVersion5,
			forceZeroStreamID: true,
		},
		{
			name:                  "v5-beta is supported / 0 stream id",
			supportedVersions:     []protoVersion{protoVersion3, protoVersion4},
			supportedBetaVersions: []protoVersion{protoVersion5},
			expectedVersion:       protoVersion4,
			forceZeroStreamID:     true,
		},
		{
			name:              "v5 is unsupported / 0 stream id",
			supportedVersions: []protoVersion{protoVersion3, protoVersion4},
			expectedVersion:   protoVersion4,
			forceZeroStreamID: true,
		},
		{
			name:             "wrong host addr",
			expectedErrorMsg: "unable to discover protocol version",
			overrideHost:     "1.2.3.4", // totally wrong addr to get network related error
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := &requestHandlerForProtocolNegotiationTest{
				supportedProtocolVersions: tc.supportedVersions,
				supportedBetaProtocols:    tc.supportedBetaVersions,
				forceZeroStreamID:         tc.forceZeroStreamID,
			}

			// use the maximum protocol supported
			protocol := uint8(0)
			for _, supportedVersion := range tc.supportedVersions {
				supportedProto := uint8(supportedVersion)
				if supportedProto > protocol {
					protocol = supportedProto
				}
			}

			srv := newTestServerOpts{
				addr:                       "127.0.0.1:0",
				protocol:                   protocol,
				customRequestHandler:       handler.handle,
				dontFailOnProtocolMismatch: true,
			}.newServer(t, context.Background())

			go srv.serve()
			defer srv.Stop()

			cluster := NewCluster(srv.Address)
			if tc.overrideHost != "" {
				cluster.Hosts = []string{tc.overrideHost}
			}

			cluster.Compressor = nil
			cluster.ProtoVersion = 0
			cluster.Logger = NewLogger(LogLevelDebug)
			cluster.ConnectTimeout = time.Second * 2
			cluster.Timeout = time.Second * 2
			cluster.DisableInitialHostLookup = true

			s, err := cluster.CreateSession()
			switch {
			case tc.expectedErrorMsg != "":
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedErrorMsg)
			default:
				require.NoError(t, err)
				require.Equal(t, tc.expectedVersion, protoVersion(s.cfg.ProtoVersion))
			}
		})
	}
}
