// segment_codec.go

package gocql

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	maxSegmentPayloadSize = 1<<17 - 1

	compressedHeaderSize   = 5 + crc24Size
	uncompressedHeaderSize = 3 + crc24Size

	crc24Size = 3
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

type segmentCodec struct {
	compressor Compressor
	compressed bool
}

func newSegmentCodec(compressor Compressor) segmentCodec {
	return segmentCodec{
		compressed: compressor != nil,
		compressor: compressor,
	}
}

func (sc *segmentCodec) encode(payload []byte, isSelfContained bool) ([]byte, error) {
	if len(payload) > maxSegmentPayloadSize {
		return nil, fmt.Errorf("gocql: payload length (%d) exceeds maximum segment size of %d", len(payload), maxSegmentPayloadSize)
	}

	if sc.compressed {
		return sc.encodeCompressedSegment(payload, isSelfContained)
	}
	return sc.encodeUncompressedSegment(payload, isSelfContained)
}

func (sc *segmentCodec) encodeCompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	uncompressedLen := len(payload)

	compressed, err := sc.compressor.AppendCompressed(nil, payload)
	if err != nil {
		return nil, err
	}

	compressedLen := len(compressed)

	// If compression is not worth it, we should send uncompressed data
	// following the next logic:
	if uncompressedLen < compressedLen {
		compressed = payload
		compressedLen = uncompressedLen
		uncompressedLen = 0
	}

	segmentBuf := make([]byte, compressedHeaderSize+compressedLen+crc32Size)

	sc.encodeCompressedSegmentHeader(compressedLen, uncompressedLen, isSelfContained, segmentBuf)
	sc.encodePayloadAndChecksum(compressed, segmentBuf[compressedHeaderSize:])

	return segmentBuf, nil
}

// encodeCompressedSegmentHeader encodes the compressed segment header into the provided destination slice.
// It assumes that dest has enough space to hold the header.
func (sc *segmentCodec) encodeCompressedSegmentHeader(compressedLen, uncompressedLen int, isSelfContained bool, dest []byte) {
	combined := uint64(compressedLen) | uint64(uncompressedLen)<<17
	if isSelfContained {
		combined |= 1 << 34
	}

	binary.LittleEndian.PutUint64(dest[:], combined)

	headerCRC24 := Crc24(dest[:5])
	dest[5] = byte(headerCRC24)
	dest[6] = byte(headerCRC24 >> 8)
	dest[7] = byte(headerCRC24 >> 16)
}

func (sc *segmentCodec) encodeUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	payloadLen := len(payload)

	segmentBuf := make([]byte, uncompressedHeaderSize+payloadLen+crc32Size)

	sc.encodeUncompressedSegmentHeader(payloadLen, isSelfContained, segmentBuf)
	sc.encodePayloadAndChecksum(payload, segmentBuf[uncompressedHeaderSize:])

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
	var headerBuf [8]byte // TODO: potentially optimize allocation, could be stored in segmentCodec and reused if the codec is a specific for each Conn

	if _, err := io.ReadFull(r, headerBuf[:8]); err != nil {
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
	var header [6]byte

	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	readHeaderCRC24 := uint32(header[3]) | uint32(header[4])<<8 | uint32(header[5])<<16
	err := sc.verifySegmentHeaderChecksum(header[:3], readHeaderCRC24)
	if err != nil {
		return nil, err
	}

	headerInt := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
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

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("gocql: failed to read segment payload crc32, err: %w", err)
	}

	readPayloadCRC32 := binary.LittleEndian.Uint32(crcBuf[:])
	err := sc.verifySegmentPayloadChecksum(payload, readPayloadCRC32)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
