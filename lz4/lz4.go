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
	"encoding/binary"
	"fmt"

	"github.com/pierrec/lz4/v4"
)

// LZ4Compressor implements the gocql.Compressor interface and can be used to
// compress incoming and outgoing frames. According to the Cassandra docs the
// LZ4 protocol should be preferred over snappy. (For details refer to
// https://cassandra.apache.org/doc/latest/operating/compression.html)
//
// Implementation note: Cassandra prefixes each compressed block with 4 bytes
// of the uncompressed block length, written in big endian order. But the LZ4
// compression library github.com/pierrec/lz4/v4 does not expect the length
// field, so it needs to be added to compressed blocks sent to Cassandra, and
// removed from ones received from Cassandra before decompression.
type LZ4Compressor struct{}

func (s LZ4Compressor) Name() string {
	return "lz4"
}

func (s LZ4Compressor) Encode(data []byte) ([]byte, error) {
	buf := make([]byte, lz4.CompressBlockBound(len(data)+4))
	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(data, buf[4:])
	// According to lz4.CompressBlock doc, it doesn't fail as long as the dst
	// buffer length is at least lz4.CompressBlockBound(len(data))) bytes, but
	// we check for error anyway just to be thorough.
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(buf, uint32(len(data)))
	return buf[:n+4], nil
}

func (s LZ4Compressor) Decode(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(data))
	}
	uncompressedLength := binary.BigEndian.Uint32(data)
	if uncompressedLength == 0 {
		return nil, nil
	}
	buf := make([]byte, uncompressedLength)
	n, err := lz4.UncompressBlock(data[4:], buf)
	return buf[:n], err
}
