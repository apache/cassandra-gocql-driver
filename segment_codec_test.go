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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMockedCompressor struct {
	// this is an error its methods should return
	expectedError error

	// invalidateDecodedDataLength allows to simulate data decoding invalidation
	invalidateDecodedDataLength bool

	// forceBiggerCompressedData allows to simulate compression that results in bigger data
	forceBiggerCompressedData bool
}

func (m testMockedCompressor) Name() string {
	return "testMockedCompressor"
}

func (m testMockedCompressor) AppendCompressed(_, src []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	if m.forceBiggerCompressedData {
		return append([]byte{1}, src...), nil
	}

	return src, nil
}

func (m testMockedCompressor) AppendDecompressed(_, src []byte, decompressedLength uint32) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	// simulating invalid size of decoded data
	if m.invalidateDecodedDataLength {
		return src[:decompressedLength-1], nil
	}

	if m.forceBiggerCompressedData {
		return src[1:], nil
	}

	return src, nil
}

func (m testMockedCompressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	panic("testMockedCompressor.AppendCompressedWithLength is not implemented")
}

func (m testMockedCompressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	panic("testMockedCompressor.AppendDecompressedWithLength is not implemented")
}

func Test_readUncompressedFrame(t *testing.T) {
	tests := []struct {
		name        string
		modifyFrame func([]byte) []byte
		expectedErr string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErr: "gocql: crc24 mismatch in segment header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErr: "gocql: payload crc32 mismatch in segment payload",
		},
		{
			name: "invalid frame length",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:7]
				return frame
			},
			expectedErr: "gocql: failed to read uncompressed segment payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:len(frame)-4]
				return frame
			},
			expectedErr: "gocql: failed to read segment payload crc32",
		},
		{
			name:        "success",
			modifyFrame: nil,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5, GlobalTypes)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			segmentCodec := newSegmentCodec(nil)
			frame, err := segmentCodec.encode(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrame != nil {
				frame = tt.modifyFrame(frame)
			}

			readFrame, isSelfContained, err := segmentCodec.decode(bytes.NewReader(frame))

			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				assert.True(t, isSelfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}

func Test_readCompressedFrame(t *testing.T) {
	tests := []struct {
		name string
		// modifyFrameFn is useful for simulating frame data invalidation
		modifyFrameFn func([]byte) []byte
		compressor    testMockedCompressor

		// expectedErrorMsg is an error message that should be returned by Error() method.
		// We need this to understand which of fmt.Errorf() is returned
		expectedErrorMsg string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc24 mismatch in segment header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErrorMsg: "gocql: payload crc32 mismatch in segment payload",
		},
		{
			name: "invalid frame length",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:12]
			},
			expectedErrorMsg: "gocql: failed to read compressed segment payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:len(frame)-4]
			},
			expectedErrorMsg: "gocql: failed to read segment payload crc32",
		},
		{
			name:          "failed to encode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to encode payload"),
			},
			expectedErrorMsg: "failed to encode payload",
		},
		{
			name:          "failed to decode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to decode payload"),
			},
			expectedErrorMsg: "failed to decode payload",
		},
		{
			name:          "length mismatch after decompressing",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				invalidateDecodedDataLength: true,
			},
			expectedErrorMsg: "gocql: length mismatch after payload decompressing",
		},
		{
			name:             "success",
			modifyFrameFn:    nil,
			expectedErrorMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5, GlobalTypes)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			segmentCodec1 := newSegmentCodec(testMockedCompressor{})
			frame, err := segmentCodec1.encode(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrameFn != nil {
				frame = tt.modifyFrameFn(frame)
			}

			segmentCodec2 := newSegmentCodec(tt.compressor)
			readFrame, selfContained, err := segmentCodec2.decode(bytes.NewReader(frame))

			switch {
			case tt.expectedErrorMsg != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrorMsg)
			case tt.compressor.expectedError != nil:
				require.ErrorIs(t, err, tt.compressor.expectedError)
			default:
				require.NoError(t, err)
				assert.True(t, selfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}

func Test_segmentCodec_encode_payloadSizeValidation(t *testing.T) {
	codec := newSegmentCodec(nil)

	// Test max valid payload
	maxPayload := make([]byte, maxSegmentPayloadSize)
	_, err := codec.encode(maxPayload, true)
	require.NoError(t, err)

	// Test exceeding max payload
	oversizedPayload := make([]byte, maxSegmentPayloadSize+1)
	_, err = codec.encode(oversizedPayload, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum segment size")
}

func Test_segmentCodec_encodeCompressedSegmentHeader(t *testing.T) {
	tests := []struct {
		name            string
		compressedLen   int
		uncompressedLen int
		isSelfContained bool
	}{
		{
			name:            "small payload self-contained",
			compressedLen:   100,
			uncompressedLen: 200,
			isSelfContained: true,
		},
		{
			name:            "small payload not self-contained",
			compressedLen:   100,
			uncompressedLen: 200,
			isSelfContained: false,
		},
		{
			name:            "max size payload",
			compressedLen:   maxSegmentPayloadSize,
			uncompressedLen: maxSegmentPayloadSize,
			isSelfContained: true,
		},
		{
			name:            "zero uncompressed length",
			compressedLen:   150,
			uncompressedLen: 0,
			isSelfContained: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := newSegmentCodec(testMockedCompressor{})
			dest := make([]byte, compressedHeaderSize)

			codec.encodeCompressedSegmentHeader(tt.compressedLen, tt.uncompressedLen, tt.isSelfContained, dest)

			header, err := codec.decodeCompressedSegmentHeader(bytes.NewReader(dest))
			require.NoError(t, err)
			assert.Equal(t, tt.compressedLen, header.payloadLength)
			assert.Equal(t, tt.uncompressedLen, header.uncompressedPayloadLength)
			assert.Equal(t, tt.isSelfContained, header.isSelfContained)
		})
	}
}

func Test_segmentCodec_encodeUncompressedSegmentHeader(t *testing.T) {
	tests := []struct {
		name            string
		payloadLen      int
		isSelfContained bool
	}{
		{
			name:            "small payload self-contained",
			payloadLen:      100,
			isSelfContained: true,
		},
		{
			name:            "small payload not self-contained",
			payloadLen:      100,
			isSelfContained: false,
		},
		{
			name:            "max size payload",
			payloadLen:      maxSegmentPayloadSize,
			isSelfContained: true,
		},
		{
			name:            "empty payload",
			payloadLen:      0,
			isSelfContained: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := newSegmentCodec(nil)
			dest := make([]byte, uncompressedHeaderSize)

			codec.encodeUncompressedSegmentHeader(tt.payloadLen, tt.isSelfContained, dest)

			header, err := codec.decodeUncompressedSegmentHeader(bytes.NewReader(dest))
			require.NoError(t, err)
			assert.Equal(t, tt.payloadLen, header.payloadLength)
			assert.Equal(t, tt.isSelfContained, header.isSelfContained)
		})
	}
}

func Test_segmentCodec_encodePayloadAndChecksum(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "small payload",
			payload: []byte("hello world"),
		},
		{
			name:    "empty payload",
			payload: []byte{},
		},
		{
			name:    "large payload",
			payload: bytes.Repeat([]byte("test"), maxSegmentPayloadSize/4),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := newSegmentCodec(nil)
			dest := make([]byte, len(tt.payload)+crc32Size)

			codec.encodePayloadAndChecksum(tt.payload, dest)

			// Verify payload is copied correctly
			assert.Equal(t, tt.payload, dest[:len(tt.payload)])

			// Verify checksum
			expectedCRC := Crc32(tt.payload)
			actualCRC := binary.LittleEndian.Uint32(dest[len(tt.payload):])
			assert.Equal(t, expectedCRC, actualCRC)
		})
	}
}

func Test_segmentCodec_encode_compressionWorthiness(t *testing.T) {
	// Test that when compression results in larger data, uncompressed is sent
	payload := []byte("small")

	// Mock compressor that returns larger data
	mockCompressor := testMockedCompressor{
		forceBiggerCompressedData: true,
	}
	codec := newSegmentCodec(mockCompressor)

	encoded, err := codec.encode(payload, true)
	require.NoError(t, err)

	reader := bytes.NewReader(encoded)

	header, err := codec.decodeCompressedSegmentHeader(reader)
	require.NoError(t, err)

	// Since compression is not worthy, the header should indicate uncompressed segment
	assert.Equal(t, len(payload), header.payloadLength)
	assert.Equal(t, 0, header.uncompressedPayloadLength)
	assert.True(t, header.isSelfContained)

	// And payload should match original, so it wasn't actually compressed
	decodedPayload, err := codec.decodePayload(reader, header)
	require.NoError(t, err)
	assert.Equal(t, payload, decodedPayload)
}

func Test_segmentCodec_roundtrip_uncompressed(t *testing.T) {
	tests := []struct {
		name            string
		payload         []byte
		isSelfContained bool
	}{
		{
			name:            "small self-contained",
			payload:         []byte("test payload"),
			isSelfContained: true,
		},
		{
			name:            "small not self-contained",
			payload:         []byte("test payload"),
			isSelfContained: false,
		},
		{
			name:            "empty payload",
			payload:         []byte{},
			isSelfContained: true,
		},
		{
			name:            "max size payload",
			payload:         bytes.Repeat([]byte("x"), maxSegmentPayloadSize),
			isSelfContained: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := newSegmentCodec(nil)

			encoded, err := codec.encode(tt.payload, tt.isSelfContained)
			require.NoError(t, err)

			decoded, selfContained, err := codec.decode(bytes.NewReader(encoded))
			require.NoError(t, err)
			assert.Equal(t, tt.payload, decoded)
			assert.Equal(t, tt.isSelfContained, selfContained)
		})
	}
}

func Test_segmentCodec_roundtrip_compressed(t *testing.T) {
	tests := []struct {
		name            string
		payload         []byte
		isSelfContained bool
	}{
		{
			name:            "small self-contained",
			payload:         []byte("test payload"),
			isSelfContained: true,
		},
		{
			name:            "small not self-contained",
			payload:         []byte("test payload"),
			isSelfContained: false,
		},
		{
			name:            "empty payload",
			payload:         []byte{},
			isSelfContained: true,
		},
		{
			name:            "large payload",
			payload:         bytes.Repeat([]byte("test data "), 1000),
			isSelfContained: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// using real lz4 compressor for this test
			codec := newSegmentCodec(lz4.LZ4Compressor{})

			encoded, err := codec.encode(tt.payload, tt.isSelfContained)
			require.NoError(t, err)

			decoded, selfContained, err := codec.decode(bytes.NewReader(encoded))
			require.NoError(t, err)
			assert.Equal(t, tt.payload, decoded)
			assert.Equal(t, tt.isSelfContained, selfContained)
		})
	}
}
