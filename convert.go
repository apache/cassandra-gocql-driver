// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"github.com/tux21b/gocql/uuid"
	"math"
	"reflect"
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

func decode(b []byte, t []uint16) driver.Value {
	switch t[0] {
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
	case typeMap:
		// A Map is stored as the number of tuples followed by the sequence of tuples.
		// Each name and value are stored as a byte count followed by the raw bytes.
		//
		// Example:
		// 0 23 - 0 3 - 1 2 3 - 0 1 - 66 .... repeats
		//
		// In this example, there are 23 tuples. The first tuple's name has 3 bytes,
		// which are 1, 2, and 3, and the tuple's value has 1 byte, which is 66.
		//
		// It only supports map[string][string] right now.
		if t[1] == typeVarchar && t[2] == typeVarchar {
			// Read the number of tuples
			collLen := int(binary.BigEndian.Uint16(b[:2]))
			coll := make(map[string]string, collLen)

			for pairNum, bpointer := 0, 2; pairNum < collLen; pairNum++ {
				// Read the byte count for the tuple's name
				keyLen := int(binary.BigEndian.Uint16(b[bpointer : bpointer+2]))
				bpointer += 2
				// Read the tuple's name according to the byte count
				key := string(b[bpointer : bpointer+keyLen])
				bpointer += keyLen

				// Read the byte count for the tuple's value
				valueLen := int(binary.BigEndian.Uint16(b[bpointer : bpointer+2]))
				bpointer += 2
				// Read the tuple's value according to the byte count
				value := string(b[bpointer : bpointer+valueLen])
				bpointer += valueLen

				coll[key] = value
			}

			return coll
		} else {
			panic("unsupported map collection type")
		}
	default:
		panic("unsupported type")
	}
	return b
}

type columnEncoder struct {
	columnTypes [][]uint16
}

func (e *columnEncoder) ColumnConverter(idx int) ValueConverter {
	switch e.columnTypes[idx][0] {
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
	case typeMap:
		return ValueConverter(encMap)
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
	x := reflect.Indirect(reflect.ValueOf(v)).Interface()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(x.(int64)))
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

func encMap(v interface{}) (driver.Value, error) {
	var err error

	// It only supports map[string][string] right now.
	if values, passed := v.(map[string]string); passed {
		buf := new(bytes.Buffer)

		// number of pairs
		err = binary.Write(buf, binary.BigEndian, uint16(len(values)))
		if err != nil {
			return nil, err
		}

		for mk, mv := range values {
			// number of pair key in bytes + key
			err = binary.Write(buf, binary.BigEndian, uint16(len(mk)))
			if err != nil {
				return nil, err
			}
			buf.Write([]byte(mk)) // err is always nil, skip the checking

			// number of pair value in bytes + value
			err = binary.Write(buf, binary.BigEndian, uint16(len(mv)))
			if err != nil {
				return nil, err
			}
			buf.Write([]byte(mv)) // err is always nil, skip the checking
		}

		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("doesn't support the given type %T", v)
}
