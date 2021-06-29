package lz4

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLZ4Compressor(t *testing.T) {
	var c LZ4Compressor
	require.Equal(t, "lz4", c.Name())

	_, err := c.Decode([]byte{0, 1, 2})
	require.EqualError(t, err, "cassandra lz4 block size should be >4, got=3")

	_, err = c.Decode([]byte{0, 1, 2, 4, 5})
	require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

	// If uncompressed size is zero then nothing is decoded even if present.
	decoded, err := c.Decode([]byte{0, 0, 0, 0, 5, 7, 8})
	require.NoError(t, err)
	require.Nil(t, decoded)

	original := []byte("My Test String")
	encoded, err := c.Encode(original)
	require.NoError(t, err)
	decoded, err = c.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}
