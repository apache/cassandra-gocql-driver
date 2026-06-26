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
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// Maximum size of a segment payload in bytes
	maxSegmentPayloadSize = 1<<17 - 1

	// Size of compressed segment header in bytes
	compressedHeaderSize = 5 + crc24Size
	// Size of uncompressed segment header in bytes
	uncompressedHeaderSize = 3 + crc24Size

	// Size of header checksum in bytes
	crc24Size = 3
	// Size of payload checksum in bytes
	crc32Size = 4
)

// segmentHeader represents the header information of a segment.
type segmentHeader struct {
	// payload length is the length of the segment payload
	payloadLength int
	// uncompressedPayloadLength is the length of the uncompressed payload (only for compressed segments)
	uncompressedPayloadLength int
	// indicates whether the segment contains only completed frames
	isSelfContained bool
}

func (segment *segmentHeader) String() string {
	return fmt.Sprintf("segmentHeader(len=%d, uncompressedLen=%d, isSelfContained=%v)",
		segment.payloadLength,
		segment.uncompressedPayloadLength,
		segment.isSelfContained)
}

// segmentCodec is responsible for encoding and decoding segments.
// It supports both compressed and uncompressed segment formats.
// Neither the encode nor the decode path is thread safe: both reuse internal scratch
// buffers, so a single segmentCodec must be used by at most one goroutine at a time.
type segmentCodec struct {
	compressor Compressor
	compressed bool
	// Reusable buffer for decoding segment header, at most 8 bytes
	readHeaderBuf [compressedHeaderSize]byte
	// Reusable buffer for decoding segment payload crc32, at most 4 bytes
	readChecksumBuf [crc32Size]byte

	// Reusable scratch buffers for the compressed encode path. encodeConcatBuf holds the
	// contiguous compressor input (the concatenated frames) and encodeCompressBuf holds the
	// compressor output. Both are fully consumed within a single encode call (copied into the
	// returned segment buffer), so reusing them across calls is safe even on the writev
	// big-frame path where multiple returned segment buffers stay alive simultaneously.
	encodeConcatBuf   []byte
	encodeCompressBuf []byte
}

func newSegmentCodec(compressor Compressor) segmentCodec {
	return segmentCodec{
		compressed: compressor != nil,
		compressor: compressor,
	}
}

// encode encodes the given frames into a single, freshly allocated segment buffer.
// Use encodeInto when a reusable output buffer is available.
func (sc *segmentCodec) encode(frames [][]byte, isSelfContained bool) ([]byte, error) {
	return sc.encodeInto(nil, frames, isSelfContained)
}

// encodeInto encodes the given frames into a single segment, reusing dst's backing array
// when it has enough capacity (otherwise a new buffer is allocated). The frames are treated
// as one logical payload: on the uncompressed path they are copied straight into the segment
// buffer, avoiding a separate concatenation buffer.
//
// The returned slice points to dst's backing array, so a caller that passes a reusable buffer
// must finish using the returned slice before the next encodeInto call that reuses the same dst. 
// 
// Pass nil for dst to always get a fresh allocation,
// which is required by the writev big-frame path where multiple encoded segments must stay
// alive simultaneously.
func (sc *segmentCodec) encodeInto(dst []byte, frames [][]byte, isSelfContained bool) ([]byte, error) {
	payloadLen := 0
	for _, frame := range frames {
		payloadLen += len(frame)
	}

	if payloadLen > maxSegmentPayloadSize {
		return nil, fmt.Errorf("gocql: payload length (%d) exceeds maximum segment size of %d", payloadLen, maxSegmentPayloadSize)
	}

	if sc.compressed {
		return sc.encodeCompressedSegment(dst, frames, payloadLen, isSelfContained)
	}
	return sc.encodeUncompressedSegment(dst, frames, payloadLen, isSelfContained)
}

func (sc *segmentCodec) encodeCompressedSegment(dst []byte, frames [][]byte, uncompressedLen int, isSelfContained bool) ([]byte, error) {
	// Block compression requires a single contiguous input buffer, so the frames have to be
	// concatenated before being handed to the compressor. Both scratch buffers are reused
	// across calls; they are fully consumed (copied into segmentBuf) before this returns.
	sc.encodeConcatBuf = appendFrames(sc.encodeConcatBuf[:0], frames)
	payload := sc.encodeConcatBuf

	compressed, err := sc.compressor.AppendCompressed(sc.encodeCompressBuf[:0], payload)
	if err != nil {
		return nil, err
	}
	sc.encodeCompressBuf = compressed

	compressedLen := len(compressed)

	// If compression is not worth it, we should send uncompressed data
	// following the next logic:
	if uncompressedLen < compressedLen {
		compressed = payload
		compressedLen = uncompressedLen
		uncompressedLen = 0
	}

	segmentBuf := resizeBuf(dst, compressedHeaderSize+compressedLen+crc32Size)

	sc.encodeCompressedSegmentHeader(compressedLen, uncompressedLen, isSelfContained, segmentBuf)
	sc.encodePayloadAndChecksum(compressed, segmentBuf[compressedHeaderSize:])

	return segmentBuf, nil
}

// appendFrames appends the frames to dst in order and returns the extended slice.
func appendFrames(dst []byte, frames [][]byte) []byte {
	for _, frame := range frames {
		dst = append(dst, frame...)
	}
	return dst
}

// resizeBuf returns a slice of length n that reuses buf's backing array when it has enough
// capacity, allocating a new buffer otherwise.
func resizeBuf(buf []byte, n int) []byte {
	if cap(buf) >= n {
		return buf[:n]
	}
	return make([]byte, n)
}

// encodeCompressedSegmentHeader encodes the compressed segment header into the provided destination slice.
// It assumes that dest has enough space to hold the header.
func (sc *segmentCodec) encodeCompressedSegmentHeader(compressedLen, uncompressedLen int, isSelfContained bool, dest []byte) {
	combined := uint64(compressedLen) | uint64(uncompressedLen)<<17
	if isSelfContained {
		combined |= 1 << 34
	}

	binary.LittleEndian.PutUint64(dest, combined)

	headerCRC24 := Crc24(dest[:5])
	dest[5] = byte(headerCRC24)
	dest[6] = byte(headerCRC24 >> 8)
	dest[7] = byte(headerCRC24 >> 16)
}

func (sc *segmentCodec) encodeUncompressedSegment(dst []byte, frames [][]byte, payloadLen int, isSelfContained bool) ([]byte, error) {
	segmentBuf := resizeBuf(dst, uncompressedHeaderSize+payloadLen+crc32Size)
	sc.encodeUncompressedSegmentHeader(payloadLen, isSelfContained, segmentBuf)

	// Frames are copied directly into the segment payload region, so no
	// separate concatenation buffer is needed.
	payload := segmentBuf[uncompressedHeaderSize : uncompressedHeaderSize+payloadLen]
	offset := 0
	for _, frame := range frames {
		offset += copy(payload[offset:], frame)
	}

	payloadCRC32 := Crc32(payload)
	binary.LittleEndian.PutUint32(segmentBuf[uncompressedHeaderSize+payloadLen:], payloadCRC32)

	return segmentBuf, nil
}

// encodeUncompressedSegmentHeader encodes the uncompressed segment header into the provided destination slice.
// It assumes that dest has enough space to hold the header.
func (sc *segmentCodec) encodeUncompressedSegmentHeader(payloadLen int, isSelfContained bool, dest []byte) {
	headerInt := uint32(payloadLen)
	if isSelfContained {
		headerInt |= 1 << 17
	}

	dest[0] = byte(headerInt)
	dest[1] = byte(headerInt >> 8)
	dest[2] = byte(headerInt >> 16)

	crc := Crc24(dest[:3])
	dest[3] = byte(crc)
	dest[4] = byte(crc >> 8)
	dest[5] = byte(crc >> 16)
}

// encodePayloadAndChecksum encodes the payload and its CRC32 checksum into the provided destination slice.
// It assumes that dest has enough space to hold the payload and checksum.
// Starting from dest[0], it writes the payload followed by its CRC32 checksum.
func (sc *segmentCodec) encodePayloadAndChecksum(payload []byte, dest []byte) {
	payloadCRC32 := Crc32(payload)
	copy(dest, payload)
	binary.LittleEndian.PutUint32(dest[len(payload):], payloadCRC32)
}

func (sc *segmentCodec) decode(r io.Reader) ([]byte, bool, error) {
	if sc.compressed {
		return sc.decodeCompressedSegment(r)
	}
	return sc.decodeUncompressedSegment(r)
}

func (sc *segmentCodec) decodeCompressedSegment(r io.Reader) ([]byte, bool, error) {
	header, err := sc.decodeCompressedSegmentHeader(r)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read compressed segment header, err: %w", err)
	}

	compressedPayload, err := sc.decodePayload(r, header)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read compressed segment payload, err: %w", err)
	}

	var uncompressedPayload []byte
	if header.uncompressedPayloadLength > 0 {
		uncompressedPayload, err = sc.compressor.AppendDecompressed(nil, compressedPayload, uint32(header.uncompressedPayloadLength))
		if err != nil {
			return nil, false, err
		}
		// Verify that the decompressed length matches the expected length
		if uint32(len(uncompressedPayload)) != uint32(header.uncompressedPayloadLength) {
			return nil, false, fmt.Errorf("gocql: length mismatch after payload decompressing, got %d, expected %d", len(uncompressedPayload), header.uncompressedPayloadLength)
		}
	} else {
		// in case when the segment was not compressed because compression was not worth it
		uncompressedPayload = compressedPayload
	}

	return uncompressedPayload, header.isSelfContained, nil
}

func (sc *segmentCodec) decodeUncompressedSegment(r io.Reader) ([]byte, bool, error) {
	header, err := sc.decodeUncompressedSegmentHeader(r)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read uncompressed segment header, err: %w", err)
	}

	payload, err := sc.decodePayload(r, header)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read uncompressed segment payload, err: %w", err)
	}

	return payload, header.isSelfContained, nil
}

// verifySegmentHeaderChecksum verifies the CRC24 checksum of the segment header.
func (sc *segmentCodec) verifySegmentHeaderChecksum(data []byte, expected uint32) error {
	computed := Crc24(data)
	if computed != expected {
		return fmt.Errorf("gocql: crc24 mismatch in segment header: expected %d, got %d", expected, computed)
	}
	return nil
}

// verifySegmentPayloadChecksum verifies the CRC32 checksum of the segment payload.
func (sc *segmentCodec) verifySegmentPayloadChecksum(data []byte, expected uint32) error {
	computed := Crc32(data)
	if computed != expected {
		return fmt.Errorf("gocql: payload crc32 mismatch in segment payload: expected %d, got %d", expected, computed)
	}
	return nil
}

// decodeCompressedSegmentHeader reads and verifies the header of a compressed segment from the given reader.
func (sc *segmentCodec) decodeCompressedSegmentHeader(r io.Reader) (*segmentHeader, error) {
	headerBuf := sc.readHeaderBuf[:compressedHeaderSize]
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	readHeaderChecksum := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16
	err := sc.verifySegmentHeaderChecksum(headerBuf[:5], readHeaderChecksum)
	if err != nil {
		return nil, err
	}

	compressedLen := uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2]&0x1)<<16
	uncompressedLen := (uint32(headerBuf[2]) >> 1) | uint32(headerBuf[3])<<7 | uint32(headerBuf[4]&0b11)<<15
	selfContained := (headerBuf[4] & 0b100) != 0

	return &segmentHeader{
		payloadLength:             int(compressedLen),
		uncompressedPayloadLength: int(uncompressedLen),
		isSelfContained:           selfContained,
	}, nil
}

// decodeUncompressedSegmentHeader reads and verifies the header of an uncompressed segment from the given reader.
func (sc *segmentCodec) decodeUncompressedSegmentHeader(r io.Reader) (*segmentHeader, error) {
	headerBuf := sc.readHeaderBuf[:uncompressedHeaderSize]
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	readHeaderCRC24 := uint32(headerBuf[3]) | uint32(headerBuf[4])<<8 | uint32(headerBuf[5])<<16
	err := sc.verifySegmentHeaderChecksum(headerBuf[:3], readHeaderCRC24)
	if err != nil {
		return nil, err
	}

	headerInt := uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])<<16
	payloadLen := int(headerInt & maxSegmentPayloadSize)
	isSelfContained := (headerInt & (1 << 17)) != 0

	return &segmentHeader{
		payloadLength:   payloadLen,
		isSelfContained: isSelfContained,
	}, nil
}

// decodePayload reads and verifies the payload of a segment from the given reader.
func (sc *segmentCodec) decodePayload(r io.Reader, header *segmentHeader) ([]byte, error) {
	payload := make([]byte, header.payloadLength)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	crcBuf := sc.readChecksumBuf[:]
	if _, err := io.ReadFull(r, crcBuf); err != nil {
		return nil, fmt.Errorf("gocql: failed to read segment payload crc32, err: %w", err)
	}

	readPayloadCRC32 := binary.LittleEndian.Uint32(crcBuf)
	err := sc.verifySegmentPayloadChecksum(payload, readPayloadCRC32)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
