// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"github.com/tux21b/gocql/uuid"
	"math"
	"strconv"
	"time"
)

const (
	typeCustom    uint16 = 0x0000
	typeAscii     uint16 = 0x0001
	typeBigInt    uint16 = 0x0002
	typeBlob      uint16 = 0x0003
	typeBool      uint16 = 0x0004
	typeCounter   uint16 = 0x0005
	typeDecimal   uint16 = 0x0006
	typeDouble    uint16 = 0x0007
	typeFloat     uint16 = 0x0008
	typeInt       uint16 = 0x0009
	typeText      uint16 = 0x000A
	typeTimestamp uint16 = 0x000B
	typeUUID      uint16 = 0x000C
	typeVarchar   uint16 = 0x000D
	typeVarint    uint16 = 0x000E
	typeTimeUUID  uint16 = 0x000F
	typeList      uint16 = 0x0020
	typeMap       uint16 = 0x0021
	typeSet       uint16 = 0x0022
)

func decode(b []byte, t uint16) driver.Value {
	switch t {
	case typeBool:
		if len(b) >= 1 && b[0] != 0 {
			return true
		}
		return false
	case typeBlob:
		return b
	case typeVarchar, typeText, typeAscii:
		return b
	case typeInt:
		return int64(int32(binary.BigEndian.Uint32(b)))
	case typeBigInt:
		return int64(binary.BigEndian.Uint64(b))
	case typeFloat:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
	case typeDouble:
		return math.Float64frombits(binary.BigEndian.Uint64(b))
	case typeTimestamp:
		t := int64(binary.BigEndian.Uint64(b))
		sec := t / 1000
		nsec := (t - sec*1000) * 1000000
		return time.Unix(sec, nsec)
	case typeUUID, typeTimeUUID:
		return uuid.FromBytes(b)
	default:
		panic("unsupported type")
	}
	return b
}

type columnEncoder struct {
	columnTypes []uint16
}

func (e *columnEncoder) ColumnConverter(idx int) ValueConverter {
	switch e.columnTypes[idx] {
	case typeInt:
		return ValueConverter(encInt)
	case typeBigInt:
		return ValueConverter(encBigInt)
	case typeFloat:
		return ValueConverter(encFloat)
	case typeDouble:
		return ValueConverter(encDouble)
	case typeBool:
		return ValueConverter(encBool)
	case typeVarchar, typeText, typeAscii:
		return ValueConverter(encVarchar)
	case typeBlob:
		return ValueConverter(encBlob)
	case typeTimestamp:
		return ValueConverter(encTimestamp)
	case typeUUID, typeTimeUUID:
		return ValueConverter(encUUID)
	}
	panic("not implemented")
}

type ValueConverter func(v interface{}) (driver.Value, error)

func (vc ValueConverter) ConvertValue(v interface{}) (driver.Value, error) {
	return vc(v)
}

func encBool(v interface{}) (driver.Value, error) {
	b, err := driver.Bool.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	if b.(bool) {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func encInt(v interface{}) (driver.Value, error) {
	x, err := driver.Int32.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(x.(int64)))
	return b, nil
}

func encBigInt(v interface{}) (driver.Value, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v.(int64)))
	return b, nil
}

func encVarchar(v interface{}) (driver.Value, error) {
	x, err := driver.String.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	return []byte(x.(string)), nil
}

func encFloat(v interface{}) (driver.Value, error) {
	x, err := driver.DefaultParameterConverter.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	var f float64
	switch x := x.(type) {
	case float64:
		f = x
	case int64:
		f = float64(x)
	case []byte:
		if f, err = strconv.ParseFloat(string(x), 64); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("can not convert %T to float64", x)
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, math.Float32bits(float32(f)))
	return b, nil
}

func encDouble(v interface{}) (driver.Value, error) {
	x, err := driver.DefaultParameterConverter.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	var f float64
	switch x := x.(type) {
	case float64:
		f = x
	case int64:
		f = float64(x)
	case []byte:
		if f, err = strconv.ParseFloat(string(x), 64); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("can not convert %T to float64", x)
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	return b, nil
}

func encTimestamp(v interface{}) (driver.Value, error) {
	x, err := driver.DefaultParameterConverter.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	var millis int64
	switch x := x.(type) {
	case time.Time:
		x = x.In(time.UTC)
		millis = x.UnixNano() / 1000000
	default:
		return nil, fmt.Errorf("can not convert %T to a timestamp", x)
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(millis))
	return b, nil
}

func encBlob(v interface{}) (driver.Value, error) {
	x, err := driver.DefaultParameterConverter.ConvertValue(v)
	if err != nil {
		return nil, err
	}
	var b []byte
	switch x := x.(type) {
	case string:
		b = []byte(x)
	case []byte:
		b = x
	default:
		return nil, fmt.Errorf("can not convert %T to a []byte", x)
	}
	return b, nil
}

func encUUID(v interface{}) (driver.Value, error) {
	var u uuid.UUID
	switch v := v.(type) {
	case string:
		var err error
		u, err = uuid.ParseUUID(v)
		if err != nil {
			return nil, err
		}
	case []byte:
		u = uuid.FromBytes(v)
	case uuid.UUID:
		u = v
	default:
		return nil, fmt.Errorf("can not convert %T to a UUID", v)
	}
	return u.Bytes(), nil
}
