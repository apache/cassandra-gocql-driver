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

package snappy

import (
	"bytes"
	"testing"

	"github.com/golang/snappy"
)

func TestSnappyCompressor(t *testing.T) {
	c := SnappyCompressor{}
	if c.Name() != "snappy" {
		t.Fatalf("expected name to be 'snappy', got %v", c.Name())
	}

	str := "My Test String"
	//Test Encoding
	expected := snappy.Encode(nil, []byte(str))
	if res, err := c.AppendCompressedWithLength(nil, []byte(str)); err != nil {
		t.Fatalf("failed to encode '%v' with error %v", str, err)
	} else if bytes.Compare(expected, res) != 0 {
		t.Fatal("failed to match the expected encoded value with the result encoded value.")
	}

	val, err := c.AppendCompressedWithLength(nil, []byte(str))
	if err != nil {
		t.Fatalf("failed to encode '%v' with error '%v'", str, err)
	}

	//Test Decoding
	if expected, err := snappy.Decode(nil, val); err != nil {
		t.Fatalf("failed to decode '%v' with error %v", val, err)
	} else if res, err := c.AppendDecompressedWithLength(nil, val); err != nil {
		t.Fatalf("failed to decode '%v' with error %v", val, err)
	} else if bytes.Compare(expected, res) != 0 {
		t.Fatal("failed to match the expected decoded value with the result decoded value.")
	}
}
