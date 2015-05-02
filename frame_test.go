package gocql

import (
	"bytes"
	"testing"
)

func TestFuzzBugs(t *testing.T) {
	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := []struct {
		input []byte
	}{
		{input: []byte("00000\xa0000")},
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test.input)

		var bw bytes.Buffer

		r := bytes.NewReader(test.input)

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

		t.Errorf("(%d) expected to fail for input %q", i, test.input)
	}
}
