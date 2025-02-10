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
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuzzBugs(t *testing.T) {
	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := [][]byte{
		[]byte("00000\xa0000"),
		[]byte("\x8000\x0e\x00\x00\x00\x000"),
		[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
		[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
		[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
			"\x00\n0000000000\x00\x14000000" +
			"00000000000000\x00\x020000" +
			"\x00\a000000000\x00\x050000000" +
			"\xff0000000000000000000" +
			"0000000"),
		[]byte("\x82\xe600\x00\x00\x00\x000"),
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x8200\x00\x00\x00\x00\x100\x00\x00\x12\x00\x00\x0000000" +
			"00000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.version))
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 2)

	framer.writeHeader(0, opStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{0x02, 0x00, 0x00, byte(opReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 2)

	head := frameHeader{
		version: 2,
		op:      opReady,
		length:  r.Len() - 8,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, 8))
	if err != nil {
		t.Fatal(err)
	}
	if head.op != opReady {
		t.Fatalf("expected to get header %v got %v", opReady, head.op)
	}
}

func Test_framer_writeExecuteFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	nowInSeconds := 123
	frame := writeExecuteFrame{
		preparedID:       []byte{1, 2, 3},
		resultMetadataID: []byte{4, 5, 6},
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		params: queryParams{
			nowInSeconds: &nowInSeconds,
			keyspace:     "test_keyspace",
		},
	}

	err := framer.writeExecuteFrame(123, frame.preparedID, frame.resultMetadataID, &frame.params, &frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	assertDeepEqual(t, "customPayload", frame.customPayload, framer.readBytesMap())
	assertDeepEqual(t, "preparedID", frame.preparedID, framer.readShortBytes())
	assertDeepEqual(t, "resultMetadataID", frame.resultMetadataID, framer.readShortBytes())
	assertDeepEqual(t, "constistency", frame.params.consistency, Consistency(framer.readShort()))

	flags := framer.readInt()
	if flags&int(flagWithNowInSeconds) != int(flagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	if flags&int(flagWithKeyspace) != int(flagWithKeyspace) {
		t.Fatal("expected flagWithKeyspace to be set, but it is not")
	}

	assertDeepEqual(t, "keyspace", frame.params.keyspace, framer.readString())
	assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.readInt())
}

func Test_framer_writeBatchFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	nowInSeconds := 123
	frame := writeBatchFrame{
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		nowInSeconds: &nowInSeconds,
	}

	err := framer.writeBatchFrame(123, &frame, frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	assertDeepEqual(t, "customPayload", frame.customPayload, framer.readBytesMap())
	assertDeepEqual(t, "typ", frame.typ, BatchType(framer.readByte()))
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.readShort()))
	assertDeepEqual(t, "consistency", frame.consistency, Consistency(framer.readShort()))

	flags := framer.readInt()
	if flags&int(flagWithNowInSeconds) != int(flagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.readInt())
}

type testMockedCompressor struct {
	// this is an error its methods should return
	expectedError error

	// invalidateDecodedDataLength allows to simulate data decoding invalidation
	invalidateDecodedDataLength bool
}

func (m testMockedCompressor) Name() string {
	return "testMockedCompressor"
}

func (m testMockedCompressor) AppendCompressed(_, src []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
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
			expectedErr: "gocql: crc24 mismatch in frame header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErr: "gocql: payload crc32 mismatch",
		},
		{
			name: "invalid frame length",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:7]
				return frame
			},
			expectedErr: "gocql: failed to read uncompressed frame payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:len(frame)-4]
				return frame
			},
			expectedErr: "gocql: failed to read payload crc32",
		},
		{
			name:        "success",
			modifyFrame: nil,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newUncompressedSegment(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrame != nil {
				frame = tt.modifyFrame(frame)
			}

			readFrame, isSelfContained, err := readUncompressedSegment(bytes.NewReader(frame))

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
			expectedErrorMsg: "gocql: crc24 mismatch in frame header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc32 mismatch in payload",
		},
		{
			name: "invalid frame length",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:12]
			},
			expectedErrorMsg: "gocql: failed to read compressed frame payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:len(frame)-4]
			},
			expectedErrorMsg: "gocql: failed to read payload crc32",
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
			name:          "length mismatch after decoding",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				invalidateDecodedDataLength: true,
			},
			expectedErrorMsg: "gocql: length mismatch after payload decoding",
		},
		{
			name:             "success",
			modifyFrameFn:    nil,
			expectedErrorMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newCompressedSegment(framer.buf, true, testMockedCompressor{})
			require.NoError(t, err)

			if tt.modifyFrameFn != nil {
				frame = tt.modifyFrameFn(frame)
			}

			readFrame, selfContained, err := readCompressedSegment(bytes.NewReader(frame), tt.compressor)

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
