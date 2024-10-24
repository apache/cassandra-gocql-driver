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

package lz4

import (
	"github.com/pierrec/lz4/v4"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLZ4Compressor(t *testing.T) {
	var c LZ4Compressor
	require.Equal(t, "lz4", c.Name())

	_, err := c.AppendDecompressedWithLength(nil, []byte{0, 1, 2})
	require.EqualError(t, err, "cassandra lz4 block size should be >4, got=3")

	_, err = c.AppendDecompressedWithLength(nil, []byte{0, 1, 2, 4, 5})
	require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

	// If uncompressed size is zero then nothing is decoded even if present.
	decoded, err := c.AppendDecompressedWithLength(nil, []byte{0, 0, 0, 0, 5, 7, 8})
	require.NoError(t, err)
	require.Nil(t, decoded)

	original := []byte("My Test String")
	encoded, err := c.AppendCompressedWithLength(nil, original)
	require.NoError(t, err)
	decoded, err = c.AppendDecompressedWithLength(nil, encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestLZ4Compressor_AppendCompressedDecompressed(t *testing.T) {
	c := LZ4Compressor{}

	invalidUncompressedLength := uint32(10)
	_, err := c.AppendDecompressed(nil, []byte{0, 1, 2, 4, 5}, invalidUncompressedLength)
	require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

	original := []byte("My Test String")
	encoded, err := c.AppendCompressed(nil, original)
	require.NoError(t, err)
	decoded, err := c.AppendDecompressed(nil, encoded, uint32(len(original)))
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestLZ4Compressor_AppendWithLengthGrowSliceWithData(t *testing.T) {
	var tests = []struct {
		name                 string
		src                  []byte
		dst                  []byte
		shouldReuseDst       bool
		decodeDst            []byte
		shouldReuseDecodeDst bool
	}{
		{
			name:      "both dst are empty",
			src:       []byte("small data"),
			dst:       nil,
			decodeDst: nil,
		},
		{
			name:      "dst is nil",
			src:       []byte("another piece of data"),
			dst:       nil,
			decodeDst: []byte("something"),
		},
		{
			name:      "decodeDst is nil",
			src:       []byte("another piece of data"),
			dst:       []byte("some"),
			decodeDst: nil,
		},
		{
			name:      "both dst are not empty",
			src:       []byte("another piece of data"),
			dst:       []byte("dst"),
			decodeDst: []byte("decodeDst"),
		},
		{
			name:                 "both dst slices have enough capacity",
			src:                  []byte("small"),
			dst:                  createBufWithCapAndData("cap=128", 128),
			shouldReuseDst:       true,
			decodeDst:            createBufWithCapAndData("cap=256", 256),
			shouldReuseDecodeDst: true,
		},
		{
			name:      "both dsts have some data and not enough capacity",
			src:       []byte("small"),
			dst:       createBufWithCapAndData("data", 6),
			decodeDst: createBufWithCapAndData("wow", 4),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressor := LZ4Compressor{}

			// Appending compressed data to dst,
			// expecting that dst still contains "test"
			result, err := compressor.AppendCompressedWithLength(tt.dst, tt.src)
			require.NoError(t, err)

			var expectedCap int
			if tt.shouldReuseDst {
				expectedCap = cap(tt.dst)
			} else {
				expectedCap = len(tt.dst) + lz4.CompressBlockBound(len(tt.src)) + dataLengthSize
			}

			require.Equal(t, expectedCap, cap(result))
			if len(tt.dst) > 0 {
				require.Equal(t, tt.dst, result[:len(tt.dst)])
			}

			result, err = compressor.AppendDecompressedWithLength(tt.decodeDst, result[len(tt.dst):])
			require.NoError(t, err)

			var expectedDecodeCap int
			if tt.shouldReuseDecodeDst {
				expectedDecodeCap = cap(tt.decodeDst)
			} else {
				expectedDecodeCap = len(tt.decodeDst) + len(tt.src)
			}

			require.Equal(t, expectedDecodeCap, cap(result))
			require.Equal(t, tt.src, result[len(tt.decodeDst):])
		})
	}
}

func TestLZ4Compressor_AppendGrowSliceWithData(t *testing.T) {
	var tests = []struct {
		name                 string
		src                  []byte
		dst                  []byte
		shouldReuseDst       bool
		decodeDst            []byte
		shouldReuseDecodeDst bool
	}{
		{
			name:      "both dst are empty",
			src:       []byte("small data"),
			dst:       nil,
			decodeDst: nil,
		},
		{
			name:      "dst is nil",
			src:       []byte("another piece of data"),
			dst:       nil,
			decodeDst: []byte("something"),
		},
		{
			name:      "decodeDst is nil",
			src:       []byte("another piece of data"),
			dst:       []byte("some"),
			decodeDst: nil,
		},
		{
			name:      "both dst are not empty",
			src:       []byte("another piece of data"),
			dst:       []byte("dst"),
			decodeDst: []byte("decodeDst"),
		},
		{
			name:                 "both dst slices have enough capacity",
			src:                  []byte("small"),
			dst:                  createBufWithCapAndData("cap=128", 128),
			shouldReuseDst:       true,
			decodeDst:            createBufWithCapAndData("cap=256", 256),
			shouldReuseDecodeDst: true,
		},
		{
			name:      "both dst slices have some data and not enough capacity",
			src:       []byte("small"),
			dst:       createBufWithCapAndData("data", 6),
			decodeDst: createBufWithCapAndData("wow", 4),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressor := LZ4Compressor{}

			// Appending compressed data to dst,
			// expecting that dst still contains "test"
			result, err := compressor.AppendCompressed(tt.dst, tt.src)
			require.NoError(t, err)

			var expectedCap int
			if tt.shouldReuseDst {
				expectedCap = cap(tt.dst)
			} else {
				expectedCap = len(tt.dst) + lz4.CompressBlockBound(len(tt.src))
			}

			require.Equal(t, expectedCap, cap(result))
			if len(tt.dst) > 0 {
				require.Equal(t, tt.dst, result[:len(tt.dst)])
			}

			uncompressedLen := uint32(len(tt.src))
			result, err = compressor.AppendDecompressed(tt.decodeDst, result[len(tt.dst):], uncompressedLen)
			require.NoError(t, err)

			var expectedDecodeCap int
			if tt.shouldReuseDst {
				expectedDecodeCap = cap(tt.decodeDst)
			} else {
				expectedDecodeCap = len(tt.decodeDst) + len(tt.src)
			}

			require.Equal(t, expectedDecodeCap, cap(result))
			require.Equal(t, tt.src, result[len(tt.decodeDst):])
		})
	}
}

func createBufWithCapAndData(data string, cap int) []byte {
	buf := make([]byte, cap)
	copy(buf, data)
	return buf[:len(data)]
}
