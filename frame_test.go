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

package gocql

import (
	"bytes"
	"os"
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
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x8200\x00\x00\x00\x00\x100\x00\x00\x12\x00\x00\x0000000" +
			"00000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.version))
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 2)

	framer.writeHeader(0, opStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{0x02, 0x00, 0x00, byte(opReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 2)

	head := frameHeader{
		version: 2,
		op:      opReady,
		length:  r.Len() - 8,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, 8))
	if err != nil {
		t.Fatal(err)
	}
	if head.op != opReady {
		t.Fatalf("expected to get header %v got %v", opReady, head.op)
	}
}

func Test_framer_writeQueryParams_nowInSeconds(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	frame := writeExecuteFrame{
		preparedID: []byte{1, 2, 3},
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		params: queryParams{
			nowInSeconds:      true,
			nowInSecondsValue: 123,
		},
	}

	err := framer.writeExecuteFrame(123, frame.preparedID, &frame.params, &frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	assertDeepEqual(t, "customPayload", frame.customPayload, framer.readBytesMap())
	assertDeepEqual(t, "preparedID", frame.preparedID, framer.readShortBytes())
	assertDeepEqual(t, "constistency", frame.params.consistency, Consistency(framer.readShort()))

	flags := framer.readInt()
	if flags&int(flagWithNowInSeconds) != int(flagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	assertDeepEqual(t, "nowInSeconds", frame.params.nowInSecondsValue, framer.readInt())
}

func Test_framer_writeBatchFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5)
	frame := writeBatchFrame{
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		nowInSeconds:      true,
		nowInSecondsValue: 123,
	}

	err := framer.writeBatchFrame(123, &frame, frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	assertDeepEqual(t, "customPayload", frame.customPayload, framer.readBytesMap())
	assertDeepEqual(t, "typ", frame.typ, BatchType(framer.readByte()))
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.readShort()))
	assertDeepEqual(t, "consistency", frame.consistency, Consistency(framer.readShort()))

	flags := framer.readInt()
	if flags&int(flagWithNowInSeconds) != int(flagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	assertDeepEqual(t, "nowInSeconds", frame.nowInSecondsValue, framer.readInt())
}
