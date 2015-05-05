package gocql

import (
	"bytes"
	"testing"
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
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		var bw bytes.Buffer

		r := bytes.NewReader(test)

		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(r, &bw, nil, 3)
		err = framer.readFrame(&head)
		if err != nil {
			continue
		}

		_, err = framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input %q", i, test)
	}
}
