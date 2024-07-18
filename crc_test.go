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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChecksumIEEE(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		expected uint32
	}{
		// expected values are manually generated using crc24 impl in Cassandra
		{
			name:     "empty buf",
			buf:      []byte{},
			expected: 1148681939,
		},
		{
			name:     "buf filled with 0",
			buf:      []byte{0, 0, 0, 0, 0},
			expected: 1178391023,
		},
		{
			name:     "buf filled with some data",
			buf:      []byte{1, 2, 3, 4, 5, 6},
			expected: 3536190002,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, ChecksumIEEE(tt.buf))
		})
	}
}

func TestKoopmanChecksum(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		expected uint32
	}{
		// expected values are manually generated using crc32 impl in Cassandra
		{
			name:     "buf filled with 0 (len 3)",
			buf:      []byte{0, 0, 0},
			expected: 8251255,
		},
		{
			name:     "buf filled with 0 (len 5)",
			buf:      []byte{0, 0, 0, 0, 0},
			expected: 11185162,
		},
		{
			name:     "buf filled with some data (len 3)",
			buf:      []byte{64, -30 & 0xff, 1},
			expected: 5891942,
		},
		{
			name:     "buf filled with some data (len 5)",
			buf:      []byte{64, -30 & 0xff, 1, 0, 0},
			expected: 8775784,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, KoopmanChecksum(tt.buf))
		})
	}
}
