// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"time"
)

// Marshaler is the interface implemented by objects that can marshal
// themselves into values understood by Cassandra.
type Marshaler interface {
	MarshalCQL(info *TypeInfo) ([]byte, error)
}

// Unmarshaler is the interface implemented by objects that can unmarshal
// a Cassandra specific description of themselves.
type Unmarshaler interface {
	UnmarshalCQL(info *TypeInfo, data []byte) error
}

// Marshal returns the CQL encoding of the value for the Cassandra
// internal type described by the info parameter.
func Marshal(info *TypeInfo, value interface{}) ([]byte, error) {
	if v, ok := value.(Marshaler); ok {
		return v.MarshalCQL(info)
	}
	switch info.Type {
	case TypeVarchar, TypeAscii, TypeBlob:
		return marshalVarchar(info, value)
	case TypeBoolean:
		return marshalBool(info, value)
	case TypeInt:
		return marshalInt(info, value)
	case TypeBigInt:
		return marshalBigInt(info, value)
	case TypeFloat:
		return marshalFloat(info, value)
	case TypeDouble:
		return marshalDouble(info, value)
	case TypeTimestamp:
		return marshalTimestamp(info, value)
	case TypeList, TypeSet:
		return marshalList(info, value)
	case TypeMap:
		return marshalMap(info, value)
	case TypeUUID:
		return marshalUUID(info, value)
	}
	// TODO(tux21b): add the remaining types
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

// Unmarshal parses the CQL encoded data based on the info parameter that
// describes the Cassandra internal data type and stores the result in the
// value pointed by value.
func Unmarshal(info *TypeInfo, data []byte, value interface{}) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}
	switch info.Type {
	case TypeVarchar, TypeAscii, TypeBlob:
		return unmarshalVarchar(info, data, value)
	case TypeBoolean:
		return unmarshalBool(info, data, value)
	case TypeInt:
		return unmarshalInt(info, data, value)
	case TypeBigInt, TypeCounter:
		return unmarshalBigInt(info, data, value)
	case TypeFloat:
		return unmarshalFloat(info, data, value)
	case TypeDouble:
		return unmarshalDouble(info, data, value)
	case TypeTimestamp:
		return unmarshalTimestamp(info, data, value)
	case TypeList, TypeSet:
		return unmarshalList(info, data, value)
	case TypeMap:
		return unmarshalMap(info, data, value)
	case TypeTimeUUID:
		return unmarshalTimeUUID(info, data, value)
	case TypeInet:
		return unmarshalInet(info, data, value)
	}
	// TODO(tux21b): add the remaining types
	return fmt.Errorf("can not unmarshal %s into %T", info, value)
}

func marshalVarchar(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	}
	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	switch {
	case k == reflect.String:
		return []byte(rv.String()), nil
	case k == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return rv.Bytes(), nil
	case k == reflect.Ptr:
		return marshalVarchar(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalVarchar(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *string:
		*v = string(data)
		return nil
	case *[]byte:
		var dataCopy []byte
		if data != nil {
			dataCopy = make([]byte, len(data))
			copy(dataCopy, data)
		}
		*v = dataCopy
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()
	switch {
	case k == reflect.String:
		rv.SetString(string(data))
		return nil
	case k == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		var dataCopy []byte
		if data != nil {
			dataCopy = make([]byte, len(data))
			copy(dataCopy, data)
		}
		rv.SetBytes(dataCopy)
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalInt(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case int:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case uint:
		if v > math.MaxInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case int64:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case uint64:
		if v > math.MaxInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case int32:
		return encInt(v), nil
	case uint32:
		if v > math.MaxInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case int16:
		return encInt(int32(v)), nil
	case uint16:
		return encInt(int32(v)), nil
	case int8:
		return encInt(int32(v)), nil
	case uint8:
		return encInt(int32(v)), nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case reflect.Ptr:
		return marshalInt(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func encInt(x int32) []byte {
	return []byte{byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
}

func unmarshalInt(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int:
		*v = int(decInt(data))
		return nil
	case *uint:
		x := decInt(data)
		if x < 0 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint(x)
		return nil
	case *int64:
		*v = int64(decInt(data))
		return nil
	case *uint64:
		x := decInt(data)
		if x < 0 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint64(x)
		return nil
	case *int32:
		*v = decInt(data)
		return nil
	case *uint32:
		x := decInt(data)
		if x < 0 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint32(x)
		return nil
	case *int16:
		x := decInt(data)
		if x < math.MinInt16 || x > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = int16(x)
		return nil
	case *uint16:
		x := decInt(data)
		if x < 0 || x > math.MaxUint16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint16(x)
		return nil
	case *int8:
		x := decInt(data)
		if x < math.MinInt8 || x > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = int8(x)
		return nil
	case *uint8:
		x := decInt(data)
		if x < 0 || x > math.MaxUint8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint8(x)
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32:
		rv.SetInt(int64(decInt(data)))
		return nil
	case reflect.Int16:
		x := decInt(data)
		if x < math.MinInt16 || x > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetInt(int64(x))
		return nil
	case reflect.Int8:
		x := decInt(data)
		if x < math.MinInt8 || x > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetInt(int64(x))
		return nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32:
		x := decInt(data)
		if x < 0 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	case reflect.Uint16:
		x := decInt(data)
		if x < 0 || x > math.MaxUint16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	case reflect.Uint8:
		x := decInt(data)
		if x < 0 || x > math.MaxUint8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decInt(x []byte) int32 {
	if len(x) != 4 {
		return 0
	}
	return int32(x[0])<<24 | int32(x[1])<<16 | int32(x[2])<<8 | int32(x[3])
}

func marshalBigInt(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case int:
		return encBigInt(int64(v)), nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return nil, marshalErrorf("marshal bigint: value %d out of range", v)
		}
		return encBigInt(int64(v)), nil
	case int64:
		return encBigInt(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, marshalErrorf("marshal bigint: value %d out of range", v)
		}
		return encBigInt(int64(v)), nil
	case int32:
		return encBigInt(int64(v)), nil
	case uint32:
		return encBigInt(int64(v)), nil
	case int16:
		return encBigInt(int64(v)), nil
	case uint16:
		return encBigInt(int64(v)), nil
	case int8:
		return encBigInt(int64(v)), nil
	case uint8:
		return encBigInt(int64(v)), nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		return encBigInt(v), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxInt64 {
			return nil, marshalErrorf("marshal bigint: value %d out of range", v)
		}
		return encBigInt(int64(v)), nil
	case reflect.Ptr:
		return marshalBigInt(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func encBigInt(x int64) []byte {
	return []byte{byte(x >> 56), byte(x >> 48), byte(x >> 40), byte(x >> 32),
		byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
}

func unmarshalBigInt(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int:
		x := decBigInt(data)
		if ^uint(0) == math.MaxUint32 && (x < math.MinInt32 || x > math.MaxInt32) {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = int(x)
		return nil
	case *uint:
		x := decBigInt(data)
		if x < 0 || (^uint(0) == math.MaxUint32 && x > math.MaxUint32) {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint(x)
		return nil
	case *int64:
		*v = decBigInt(data)
		return nil
	case *uint64:
		x := decBigInt(data)
		if x < 0 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint64(x)
		return nil
	case *int32:
		x := decBigInt(data)
		if x < math.MinInt32 || x > math.MaxInt32 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = int32(x)
		return nil
	case *uint32:
		x := decBigInt(data)
		if x < 0 || x > math.MaxUint32 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint32(x)
		return nil
	case *int16:
		x := decBigInt(data)
		if x < math.MinInt16 || x > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = int16(x)
		return nil
	case *uint16:
		x := decBigInt(data)
		if x < 0 || x > math.MaxUint16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint16(x)
		return nil
	case *int8:
		x := decBigInt(data)
		if x < math.MinInt8 || x > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = int8(x)
		return nil
	case *uint8:
		x := decBigInt(data)
		if x < 0 || x > math.MaxUint8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		*v = uint8(x)
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Int:
		x := decBigInt(data)
		if ^uint(0) == math.MaxUint32 && (x < math.MinInt32 || x > math.MaxInt32) {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetInt(x)
		return nil
	case reflect.Int64:
		rv.SetInt(decBigInt(data))
		return nil
	case reflect.Int32:
		x := decBigInt(data)
		if x < math.MinInt32 || x > math.MaxInt32 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetInt(x)
		return nil
	case reflect.Int16:
		x := decBigInt(data)
		if x < math.MinInt16 || x > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetInt(x)
		return nil
	case reflect.Int8:
		x := decBigInt(data)
		if x < math.MinInt8 || x > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetInt(x)
		return nil
	case reflect.Uint:
		x := decBigInt(data)
		if x < 0 || (^uint(0) == math.MaxUint32 && x > math.MaxUint32) {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	case reflect.Uint64:
		x := decBigInt(data)
		if x < 0 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	case reflect.Uint32:
		x := decBigInt(data)
		if x < 0 || x > math.MaxUint32 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	case reflect.Uint16:
		x := decBigInt(data)
		if x < 0 || x > math.MaxUint16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	case reflect.Uint8:
		x := decBigInt(data)
		if x < 0 || x > math.MaxUint8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", x)
		}
		rv.SetUint(uint64(x))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decBigInt(data []byte) int64 {
	if len(data) != 8 {
		return 0
	}
	return int64(data[0])<<56 | int64(data[1])<<48 |
		int64(data[2])<<40 | int64(data[3])<<32 |
		int64(data[4])<<24 | int64(data[5])<<16 |
		int64(data[6])<<8 | int64(data[7])
}

func marshalBool(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case bool:
		return encBool(v), nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Bool:
		return encBool(rv.Bool()), nil
	case reflect.Ptr:
		return marshalBool(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func encBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func unmarshalBool(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *bool:
		*v = decBool(data)
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Bool:
		rv.SetBool(decBool(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decBool(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	return v[0] != 0
}

func marshalFloat(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case float32:
		return encInt(int32(math.Float32bits(v))), nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Float32:
		return encInt(int32(math.Float32bits(float32(rv.Float())))), nil
	case reflect.Ptr:
		return marshalFloat(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalFloat(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float32:
		*v = math.Float32frombits(uint32(decInt(data)))
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Float32:
		rv.SetFloat(float64(math.Float32frombits(uint32(decInt(data)))))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalDouble(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case float64:
		return encBigInt(int64(math.Float64bits(v))), nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Float64:
		return encBigInt(int64(math.Float64bits(rv.Float()))), nil
	case reflect.Ptr:
		return marshalFloat(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalDouble(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float64:
		*v = math.Float64frombits(uint64(decBigInt(data)))
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Float64:
		rv.SetFloat(math.Float64frombits(uint64(decBigInt(data))))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalTimestamp(info *TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case int64:
		return encBigInt(v), nil
	case time.Time:
		x := v.In(time.UTC).UnixNano() / int64(time.Millisecond)
		return encBigInt(x), nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return encBigInt(rv.Int()), nil
	case reflect.Ptr:
		return marshalTimestamp(info, rv.Elem().Interface())
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalTimestamp(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = decBigInt(data)
		return nil
	case *time.Time:
		x := decBigInt(data)
		sec := x / 1000
		nsec := (x - sec*1000) * 1000000
		*v = time.Unix(sec, nsec).In(time.UTC)
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Int64:
		rv.SetInt(decBigInt(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalList(info *TypeInfo, value interface{}) ([]byte, error) {
	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	switch k {
	case reflect.Slice, reflect.Array:
		if k == reflect.Slice && rv.IsNil() {
			return nil, nil
		}
		buf := &bytes.Buffer{}
		n := rv.Len()
		if n > math.MaxUint16 {
			return nil, marshalErrorf("marshal: slice / array too large")
		}
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
		for i := 0; i < n; i++ {
			item, err := Marshal(info.Elem, rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			if len(item) > math.MaxUint16 {
				return nil, marshalErrorf("marshal: slice / array item too large")
			}
			buf.WriteByte(byte(len(item) >> 8))
			buf.WriteByte(byte(len(item)))
			buf.Write(item)
		}
		return buf.Bytes(), nil
	}
	if k == reflect.Map {
		elem := t.Elem()
		if elem.Kind() == reflect.Struct && elem.NumField() == 0 {
			rkeys := rv.MapKeys()
			keys := make([]interface{}, len(rkeys))
			for i := 0; i < len(keys); i++ {
				keys[i] = rkeys[i].Interface()
			}
			return marshalList(info, keys)
		}
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalList(info *TypeInfo, data []byte, value interface{}) error {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Slice, reflect.Array:
		if data == nil {
			if k == reflect.Array {
				return unmarshalErrorf("unmarshal list: can not store nil in array value")
			}
			rv.Set(reflect.Zero(t))
			return nil
		}
		if len(data) < 2 {
			return unmarshalErrorf("unmarshal list: unexpected eof")
		}
		n := int(data[0]<<8) | int(data[1])
		data = data[2:]
		if k == reflect.Array {
			if rv.Len() != n {
				return unmarshalErrorf("unmarshal list: array with wrong size")
			}
		} else if rv.Cap() < n {
			rv.Set(reflect.MakeSlice(t, n, n))
		} else {
			rv.SetLen(n)
		}
		for i := 0; i < n; i++ {
			if len(data) < 2 {
				return unmarshalErrorf("unmarshal list: unexpected eof")
			}
			m := int(data[0]<<8) | int(data[1])
			data = data[2:]
			if err := Unmarshal(info.Elem, data[:m], rv.Index(i).Addr().Interface()); err != nil {
				return err
			}
			data = data[m:]
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalMap(info *TypeInfo, value interface{}) ([]byte, error) {
	rv := reflect.ValueOf(value)
	t := rv.Type()
	if t.Kind() != reflect.Map {
		return nil, marshalErrorf("can not marshal %T into %s", value, info)
	}
	if rv.IsNil() {
		return nil, nil
	}
	buf := &bytes.Buffer{}
	n := rv.Len()
	if n > math.MaxUint16 {
		return nil, marshalErrorf("marshal: map too large")
	}
	buf.WriteByte(byte(n >> 8))
	buf.WriteByte(byte(n))
	keys := rv.MapKeys()
	for _, key := range keys {
		item, err := Marshal(info.Key, key.Interface())
		if err != nil {
			return nil, err
		}
		if len(item) > math.MaxUint16 {
			return nil, marshalErrorf("marshal: slice / array item too large")
		}
		buf.WriteByte(byte(len(item) >> 8))
		buf.WriteByte(byte(len(item)))
		buf.Write(item)

		item, err = Marshal(info.Elem, rv.MapIndex(key).Interface())
		if err != nil {
			return nil, err
		}
		if len(item) > math.MaxUint16 {
			return nil, marshalErrorf("marshal: slice / array item too large")
		}
		buf.WriteByte(byte(len(item) >> 8))
		buf.WriteByte(byte(len(item)))
		buf.Write(item)
	}
	return buf.Bytes(), nil
}

func unmarshalMap(info *TypeInfo, data []byte, value interface{}) error {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	if t.Kind() != reflect.Map {
		return unmarshalErrorf("can not unmarshal %s into %T", info, value)
	}
	if data == nil {
		rv.Set(reflect.Zero(t))
		return nil
	}
	rv.Set(reflect.MakeMap(t))
	if len(data) < 2 {
		return unmarshalErrorf("unmarshal map: unexpected eof")
	}
	n := int(data[1]) | int(data[0])<<8
	data = data[2:]
	for i := 0; i < n; i++ {
		if len(data) < 2 {
			return unmarshalErrorf("unmarshal list: unexpected eof")
		}
		m := int(data[1]) | int(data[0])<<8
		data = data[2:]
		key := reflect.New(t.Key())
		if err := Unmarshal(info.Key, data[:m], key.Interface()); err != nil {
			return err
		}
		data = data[m:]

		m = int(data[1]) | int(data[0])<<8
		data = data[2:]
		val := reflect.New(t.Elem())
		if err := Unmarshal(info.Elem, data[:m], val.Interface()); err != nil {
			return err
		}
		data = data[m:]

		rv.SetMapIndex(key.Elem(), val.Elem())
	}
	return nil
}

func marshalUUID(info *TypeInfo, value interface{}) ([]byte, error) {
	if val, ok := value.([]byte); ok && len(val) == 16 {
		return val, nil
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalTimeUUID(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *time.Time:
		if len(data) != 16 {
			return unmarshalErrorf("invalid timeuuid")
		}
		if version := int(data[6] & 0xF0 >> 4); version != 1 {
			return unmarshalErrorf("invalid timeuuid")
		}
		timestamp := int64(uint64(data[0])<<24|uint64(data[1])<<16|
			uint64(data[2])<<8|uint64(data[3])) +
			int64(uint64(data[4])<<40|uint64(data[5])<<32) +
			int64(uint64(data[6]&0x0F)<<56|uint64(data[7])<<48)
		timestamp = timestamp - timeEpoch
		sec := timestamp / 1e7
		nsec := timestamp - sec
		*v = time.Unix(int64(sec), int64(nsec)).UTC()
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalInet(info *TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *string:
		if len(data) == 0 {
			*v = ""
			return nil
		}
		if len(data) == 4 {
			*v = fmt.Sprintf("%d.%d.%d.%d", data[0], data[1], data[2], data[3])
			return nil
		}
		// TODO: support IPv6
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

// TypeInfo describes a Cassandra specific data type.
type TypeInfo struct {
	Type   Type
	Key    *TypeInfo // only used for TypeMap
	Elem   *TypeInfo // only used for TypeMap, TypeList and TypeSet
	Custom string    // only used for TypeCostum
}

// String returns a human readable name for the Cassandra datatype
// described by t.
func (t TypeInfo) String() string {
	switch t.Type {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", t.Type, t.Key, t.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", t.Type, t.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", t.Type, t.Elem)
	}
	return t.Type.String()
}

// Type is the identifier of a Cassandra internal datatype.
type Type int

const (
	TypeCustom    Type = 0x0000
	TypeAscii     Type = 0x0001
	TypeBigInt    Type = 0x0002
	TypeBlob      Type = 0x0003
	TypeBoolean   Type = 0x0004
	TypeCounter   Type = 0x0005
	TypeDecimal   Type = 0x0006
	TypeDouble    Type = 0x0007
	TypeFloat     Type = 0x0008
	TypeInt       Type = 0x0009
	TypeTimestamp Type = 0x000B
	TypeUUID      Type = 0x000C
	TypeVarchar   Type = 0x000D
	TypeVarint    Type = 0x000E
	TypeTimeUUID  Type = 0x000F
	TypeInet      Type = 0x0010
	TypeList      Type = 0x0020
	TypeMap       Type = 0x0021
	TypeSet       Type = 0x0022
)

// String returns the name of the identifier.
func (t Type) String() string {
	switch t {
	case TypeCustom:
		return "custom"
	case TypeAscii:
		return "ascii"
	case TypeBigInt:
		return "bigint"
	case TypeBlob:
		return "blob"
	case TypeBoolean:
		return "boolean"
	case TypeCounter:
		return "counter"
	case TypeDecimal:
		return "decimal"
	case TypeDouble:
		return "double"
	case TypeFloat:
		return "float"
	case TypeInt:
		return "int"
	case TypeTimestamp:
		return "timestamp"
	case TypeUUID:
		return "uuid"
	case TypeVarchar:
		return "varchar"
	case TypeTimeUUID:
		return "timeuuid"
	case TypeInet:
		return "inet"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	default:
		return "unknown"
	}
}

type MarshalError string

func (m MarshalError) Error() string {
	return string(m)
}

func marshalErrorf(format string, args ...interface{}) MarshalError {
	return MarshalError(fmt.Sprintf(format, args...))
}

type UnmarshalError string

func (m UnmarshalError) Error() string {
	return string(m)
}

func unmarshalErrorf(format string, args ...interface{}) UnmarshalError {
	return UnmarshalError(fmt.Sprintf(format, args...))
}

var timeEpoch int64 = 0x01B21DD213814000
