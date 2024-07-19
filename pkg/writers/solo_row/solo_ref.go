// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package solo_row

import (
	"fmt"
	"github.com/gocql/gocql/pkg/writers"
	"io"
	"strings"
)

func InitSoloRefWriter(rowRef writers.Struct, columnsInRow int) ToStruct {
	return ToStruct{
		row:     rowRef,
		columns: columnsInRow,
	}
}

// ToStruct optimized for responses with only one row.
// implement Row
type ToStruct struct {
	row     writers.Struct
	columns int
}

func (r *ToStruct) ColumnsInRow() int {
	return r.columns
}

// ReUse puts new reference into scanner. Useful for scanner reuse.
func (r *ToStruct) ReUse(rowRef writers.Struct) {
	r.row = rowRef
}

func (r *ToStruct) WriteRow(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
		}
	}()
	write = r.row.UnmarshalCQL2(data)
	return
}
