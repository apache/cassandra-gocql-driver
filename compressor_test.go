package gocql

import (
	"bytes"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func TestSnappyCompressor(t *testing.T) {
	c := SnappyCompressor{}
	if c.Name() != "snappy" {
		t.Fatalf("expected name to be 'snappy', got %v", c.Name())
	}

	str := "My Test String"
	//Test Encoding
	expected := snappy.Encode(nil, []byte(str))
	if res, err := c.Encode([]byte(str)); err != nil {
		t.Fatalf("failed to encode '%v' with error %v", str, err)
	} else if bytes.Compare(expected, res) != 0 {
		t.Fatal("failed to match the expected encoded value with the result encoded value.")
	}

	val, err := c.Encode([]byte(str))
	if err != nil {
		t.Fatalf("failed to encode '%v' with error '%v'", str, err)
	}

	//Test Decoding
	if expected, err := snappy.Decode(nil, val); err != nil {
		t.Fatalf("failed to decode '%v' with error %v", val, err)
	} else if res, err := c.Decode(val); err != nil {
		t.Fatalf("failed to decode '%v' with error %v", val, err)
	} else if bytes.Compare(expected, res) != 0 {
		t.Fatal("failed to match the expected decoded value with the result decoded value.")
	}
}

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
