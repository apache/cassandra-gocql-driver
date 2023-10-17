// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

import (
	"errors"
	"math/rand"
	"time"
)

func dec2ToInt16Err(data []byte) (int16, error) {
	if len(data) < 2 {
		return 0, errors.New("low len")
	}
	return int16(data[0])<<8 | int16(data[1]), nil
}

func dec2ToInt16(data []byte) int16 {
	return int16(data[0])<<8 | int16(data[1])
}

func randBytes(bytesLen int) []byte {
	out := make([]byte, bytesLen)
	for idx := range out {
		out[idx] = byte(rnd.Intn(256))
	}
	return out
}

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
