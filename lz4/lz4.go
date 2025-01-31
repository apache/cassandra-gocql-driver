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

const dataLengthSize = 4

func (s LZ4Compressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	maxLength := lz4.CompressBlockBound(len(src))
	oldDstLen := len(dst)
	dst = grow(dst, maxLength+dataLengthSize)

	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(src, dst[oldDstLen+dataLengthSize:])
	// According to lz4.CompressBlock doc, it doesn't fail as long as the dst
	// buffer length is at least lz4.CompressBlockBound(len(data))) bytes, but
	// we check for error anyway just to be thorough.
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(dst[oldDstLen:oldDstLen+dataLengthSize], uint32(len(src)))
	return dst[:oldDstLen+n+dataLengthSize], nil
}

func (s LZ4Compressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	if len(src) < dataLengthSize {
		return nil, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(src))
	}
	uncompressedLength := binary.BigEndian.Uint32(src[:dataLengthSize])
	if uncompressedLength == 0 {
		return nil, nil
	}
	oldDstLen := len(dst)
	dst = grow(dst, int(uncompressedLength))
	n, err := lz4.UncompressBlock(src[dataLengthSize:], dst[oldDstLen:])
	return dst[:oldDstLen+n], err

}

func (s LZ4Compressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	maxLength := lz4.CompressBlockBound(len(src))
	oldDstLen := len(dst)
	dst = grow(dst, maxLength)

	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(src, dst[oldDstLen:])
	if err != nil {
		return nil, err
	}

	return dst[:oldDstLen+n], nil
}

func (s LZ4Compressor) AppendDecompressed(dst, src []byte, uncompressedLength uint32) ([]byte, error) {
	if uncompressedLength == 0 {
		return nil, nil
	}
	oldDstLen := len(dst)
	dst = grow(dst, int(uncompressedLength))
	n, err := lz4.UncompressBlock(src, dst[oldDstLen:])
	return dst[:oldDstLen+n], err
}

// grow grows b to guaranty space for n elements, if needed.
func grow(b []byte, n int) []byte {
	oldLen := len(b)
	if cap(b)-oldLen < n {
		newBuf := make([]byte, oldLen+n)
		copy(newBuf, b)
		b = newBuf
	}
	return b[:oldLen+n]
}
