// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package row

import (
	"fmt"
	"github.com/gocql/gocql/pkg/writers"
	"io"
	"strings"
)

func InitToMapWriter(rows writers.Map, columnsInRow int) writers.Rows {
	return &ToMapWriter{
		rows:    rows,
		columns: columnsInRow,
	}
}

// ToMapWriter it`s implementation of writers.Rows interface, designed for write rows to the Map`s.
type ToMapWriter struct {
	rows    writers.Map
	columns int
}

func (r *ToMapWriter) ColumnsInRow() int {
	return r.columns
}

func (r *ToMapWriter) Prepare(_ int32) {
}

func (r *ToMapWriter) ReUse(rows writers.Map) {
	r.rows = rows
}

func (r *ToMapWriter) WriteRow(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
			r.rows.OnMapElemError()
		}
	}()
	write = r.rows.UnmarshalMapElem(data)
	return
}

func (r *ToMapWriter) WriteRows(data []byte) (write int32, err error) {
	// In usual case don`t need to check data on every read operation, because if unmarshalers already tested and data r right we should just read.
	// In case wrong data or wrong unmarshaler will result to wrong all rows in response, that`s why we can just catch panic once and make error.
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%T", errF), "runtime.boundsError") {
				err = io.ErrUnexpectedEOF
			} else {
				err = fmt.Errorf("%s", errF)
			}
			r.rows.OnMapElemError()
		}
	}()
	for int(write) < len(data) {
		write += r.rows.UnmarshalMapElem(data[write:])
	}
	return
}
