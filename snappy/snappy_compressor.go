package snappy

import "github.com/golang/snappy"

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. The snappy compression algorithm
// aims for very high speeds and reasonable compression.
type SnappyCompressor struct{}

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	return snappy.Encode(dst, src), nil
}

func (s SnappyCompressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func (s SnappyCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	panic("SnappyCompressor.AppendCompressed is not supported")
}

func (s SnappyCompressor) AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error) {
	panic("SnappyCompressor.AppendDecompressed is not supported")
}
