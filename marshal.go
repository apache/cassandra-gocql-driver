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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql/internal"
)

var (
	ErrorUDTUnavailable = errors.New("UDT are not available on protocols less than 3, please update config")
)

// Marshaler is the interface implemented by objects that can marshal
// themselves into values understood by Cassandra.
type Marshaler interface {
	MarshalCQL(info TypeInfo) ([]byte, error)
}

// Unmarshaler is the interface implemented by objects that can unmarshal
// a Cassandra specific description of themselves.
type Unmarshaler interface {
	UnmarshalCQL(info TypeInfo, data []byte) error
}

// Marshal returns the CQL encoding of the value for the Cassandra
// internal type described by the info parameter.
//
// nil is serialized as CQL null.
// If value implements Marshaler, its MarshalCQL method is called to marshal the data.
// If value is a pointer, the pointed-to value is marshaled.
//
// Supported conversions are as follows, other type combinations may be added in the future:
//
//	CQL type                    | Go type (value)    | Note
//	varchar, ascii, blob, text  | string, []byte     |
//	boolean                     | bool               |
//	tinyint, smallint, int      | integer types      |
//	tinyint, smallint, int      | string             | formatted as base 10 number
//	bigint, counter             | integer types      |
//	bigint, counter             | big.Int            |
//	bigint, counter             | string             | formatted as base 10 number
//	float                       | float32            |
//	double                      | float64            |
//	decimal                     | inf.Dec            |
//	time                        | int64              | nanoseconds since start of day
//	time                        | time.Duration      | duration since start of day
//	timestamp                   | int64              | milliseconds since Unix epoch
//	timestamp                   | time.Time          |
//	list, set                   | slice, array       |
//	list, set                   | map[X]struct{}     |
//	map                         | map[X]Y            |
//	uuid, timeuuid              | gocql.UUID         |
//	uuid, timeuuid              | [16]byte           | raw UUID bytes
//	uuid, timeuuid              | []byte             | raw UUID bytes, length must be 16 bytes
//	uuid, timeuuid              | string             | hex representation, see ParseUUID
//	varint                      | integer types      |
//	varint                      | big.Int            |
//	varint                      | string             | value of number in decimal notation
//	inet                        | net.IP             |
//	inet                        | string             | IPv4 or IPv6 address string
//	tuple                       | slice, array       |
//	tuple                       | struct             | fields are marshaled in order of declaration
//	user-defined type           | gocql.UDTMarshaler | MarshalUDT is called
//	user-defined type           | map[string]interface{} |
//	user-defined type           | struct             | struct fields' cql tags are used for column names
//	date                        | int64              | milliseconds since Unix epoch to start of day (in UTC)
//	date                        | time.Time          | start of day (in UTC)
//	date                        | string             | parsed using "2006-01-02" format
//	duration                    | int64              | duration in nanoseconds
//	duration                    | time.Duration      |
//	duration                    | gocql.Duration     |
//	duration                    | string             | parsed with time.ParseDuration
func Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	if info.Version() < protoVersion1 {
		panic("protocol version not set")
	}

	if valueRef := reflect.ValueOf(value); valueRef.Kind() == reflect.Ptr {
		if valueRef.IsNil() {
			return nil, nil
		} else if v, ok := value.(Marshaler); ok {
			return v.MarshalCQL(info)
		} else {
			return Marshal(info, valueRef.Elem().Interface())
		}
	}

	if v, ok := value.(Marshaler); ok {
		return v.MarshalCQL(info)
	}

	// TODO: move to internal
	// Notice: a lot of marshal functions could be moved to internal package,
	// if the Marshaler case and internal.UnsetColumn cases will be moved to this level
	switch info.Type() {
	case TypeVarchar, TypeAscii, TypeBlob, TypeText:
		return internal.MarshalVarchar(info, value)
	case TypeBoolean:
		return internal.MarshalBool(info, value)
	case TypeTinyInt:
		return internal.MarshalTinyInt(info, value)
	case TypeSmallInt:
		return internal.MarshalSmallInt(info, value)
	case TypeInt:
		return internal.MarshalInt(info, value)
	case TypeBigInt, TypeCounter:
		return internal.MarshalBigInt(info, value)
	case TypeFloat:
		return internal.MarshalFloat(info, value)
	case TypeDouble:
		return internal.MarshalDouble(info, value)
	case TypeDecimal:
		return internal.MarshalDecimal(info, value)
	case TypeTime:
		return internal.MarshalTime(info, value)
	case TypeTimestamp:
		return internal.MarshalTimestamp(info, value)
	case TypeList, TypeSet:
		return marshalList(info, value)
	case TypeMap:
		return marshalMap(info, value)
	case TypeUUID, TypeTimeUUID:
		return marshalUUID(info, value)
	case TypeVarint:
		return internal.MarshalVarint(info, value)
	case TypeInet:
		return internal.MarshalInet(info, value)
	case TypeTuple:
		return marshalTuple(info, value)
	case TypeUDT:
		return marshalUDT(info, value)
	case TypeDate:
		return internal.MarshalDate(info, value)
	case TypeDuration:
		return marshalDuration(info, value)
	}

	// detect protocol 2 UDT
	if strings.HasPrefix(info.Custom(), "org.apache.cassandra.db.marshal.UserType") && info.Version() < 3 {
		return nil, ErrorUDTUnavailable
	}

	// TODO(tux21b): add the remaining types
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

// Unmarshal parses the CQL encoded data based on the info parameter that
// describes the Cassandra internal data type and stores the result in the
// value pointed by value.
//
// If value implements Unmarshaler, it's UnmarshalCQL method is called to
// unmarshal the data.
// If value is a pointer to pointer, it is set to nil if the CQL value is
// null. Otherwise, nulls are unmarshalled as zero value.
//
// Supported conversions are as follows, other type combinations may be added in the future:
//
//	CQL type                                | Go type (value)         | Note
//	varchar, ascii, blob, text              | *string                 |
//	varchar, ascii, blob, text              | *[]byte                 | non-nil buffer is reused
//	bool                                    | *bool                   |
//	tinyint, smallint, int, bigint, counter | *integer types          |
//	tinyint, smallint, int, bigint, counter | *big.Int                |
//	tinyint, smallint, int, bigint, counter | *string                 | formatted as base 10 number
//	float                                   | *float32                |
//	double                                  | *float64                |
//	decimal                                 | *inf.Dec                |
//	time                                    | *int64                  | nanoseconds since start of day
//	time                                    | *time.Duration          |
//	timestamp                               | *int64                  | milliseconds since Unix epoch
//	timestamp                               | *time.Time              |
//	list, set                               | *slice, *array          |
//	map                                     | *map[X]Y                |
//	uuid, timeuuid                          | *string                 | see UUID.String
//	uuid, timeuuid                          | *[]byte                 | raw UUID bytes
//	uuid, timeuuid                          | *gocql.UUID             |
//	timeuuid                                | *time.Time              | timestamp of the UUID
//	inet                                    | *net.IP                 |
//	inet                                    | *string                 | IPv4 or IPv6 address string
//	tuple                                   | *slice, *array          |
//	tuple                                   | *struct                 | struct fields are set in order of declaration
//	user-defined types                      | gocql.UDTUnmarshaler    | UnmarshalUDT is called
//	user-defined types                      | *map[string]interface{} |
//	user-defined types                      | *struct                 | cql tag is used to determine field name
//	date                                    | *time.Time              | time of beginning of the day (in UTC)
//	date                                    | *string                 | formatted with 2006-01-02 format
//	duration                                | *gocql.Duration         |
func Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}

	if isNullableValue(value) {
		return unmarshalNullable(info, data, value)
	}

	switch info.Type() {
	case TypeVarchar, TypeAscii, TypeBlob, TypeText:
		return unmarshalVarchar(info, data, value)
	case TypeBoolean:
		return unmarshalBool(info, data, value)
	case TypeInt:
		return unmarshalInt(info, data, value)
	case TypeBigInt, TypeCounter:
		return unmarshalBigInt(info, data, value)
	case TypeVarint:
		return unmarshalVarint(info, data, value)
	case TypeSmallInt:
		return unmarshalSmallInt(info, data, value)
	case TypeTinyInt:
		return unmarshalTinyInt(info, data, value)
	case TypeFloat:
		return unmarshalFloat(info, data, value)
	case TypeDouble:
		return unmarshalDouble(info, data, value)
	case TypeDecimal:
		return unmarshalDecimal(info, data, value)
	case TypeTime:
		return unmarshalTime(info, data, value)
	case TypeTimestamp:
		return unmarshalTimestamp(info, data, value)
	case TypeList, TypeSet:
		return unmarshalList(info, data, value)
	case TypeMap:
		return unmarshalMap(info, data, value)
	case TypeTimeUUID:
		return unmarshalTimeUUID(info, data, value)
	case TypeUUID:
		return unmarshalUUID(info, data, value)
	case TypeInet:
		return unmarshalInet(info, data, value)
	case TypeTuple:
		return unmarshalTuple(info, data, value)
	case TypeUDT:
		return unmarshalUDT(info, data, value)
	case TypeDate:
		return unmarshalDate(info, data, value)
	case TypeDuration:
		return unmarshalDuration(info, data, value)
	}

	// detect protocol 2 UDT
	if strings.HasPrefix(info.Custom(), "org.apache.cassandra.db.marshal.UserType") && info.Version() < 3 {
		return ErrorUDTUnavailable
	}

	// TODO(tux21b): add the remaining types
	return fmt.Errorf("can not unmarshal %s into %T", info, value)
}

func isNullableValue(value interface{}) bool {
	v := reflect.ValueOf(value)
	return v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Ptr
}

func isNullData(info TypeInfo, data []byte) bool {
	return data == nil
}

func unmarshalNullable(info TypeInfo, data []byte, value interface{}) error {
	valueRef := reflect.ValueOf(value)

	if isNullData(info, data) {
		nilValue := reflect.Zero(valueRef.Type().Elem())
		valueRef.Elem().Set(nilValue)
		return nil
	}

	newValue := reflect.New(valueRef.Type().Elem().Elem())
	valueRef.Elem().Set(newValue)
	return Unmarshal(info, data, newValue.Interface())
}

func unmarshalVarchar(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *string:
		*v = string(data)
		return nil
	case *[]byte:
		if data != nil {
			*v = append((*v)[:0], data...)
		} else {
			*v = nil
		}
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

func unmarshalBigInt(info TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, internal.DecBigInt(data), data, value)
}

func unmarshalInt(info TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, int64(internal.DecInt(data)), data, value)
}

func unmarshalSmallInt(info TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, int64(internal.DecShort(data)), data, value)
}

func unmarshalTinyInt(info TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, int64(internal.DecTiny(data)), data, value)
}

func unmarshalVarint(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case *big.Int:
		return unmarshalIntlike(info, 0, data, value)
	case *uint64:
		if len(data) == 9 && data[0] == 0 {
			*v = internal.BytesToUint64(data[1:])
			return nil
		}
	}

	if len(data) > 8 {
		return unmarshalErrorf("unmarshal int: varint value %v out of range for %T (use big.Int)", data, value)
	}

	int64Val := internal.BytesToInt64(data)
	if len(data) > 0 && len(data) < 8 && data[0]&0x80 > 0 {
		int64Val -= (1 << uint(len(data)*8))
	}
	return unmarshalIntlike(info, int64Val, data, value)
}

func unmarshalIntlike(info TypeInfo, int64Val int64, data []byte, value interface{}) error {
	switch v := value.(type) {
	case *int:
		if ^uint(0) == math.MaxUint32 && (int64Val < math.MinInt32 || int64Val > math.MaxInt32) {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int(int64Val)
		return nil
	case *uint:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case TypeInt:
			*v = uint(unitVal) & 0xFFFFFFFF
		case TypeSmallInt:
			*v = uint(unitVal) & 0xFFFF
		case TypeTinyInt:
			*v = uint(unitVal) & 0xFF
		default:
			if ^uint(0) == math.MaxUint32 && (int64Val < 0 || int64Val > math.MaxUint32) {
				return unmarshalErrorf("unmarshal int: value %d out of range for %T", unitVal, *v)
			}
			*v = uint(unitVal)
		}
		return nil
	case *int64:
		*v = int64Val
		return nil
	case *uint64:
		switch info.Type() {
		case TypeInt:
			*v = uint64(int64Val) & 0xFFFFFFFF
		case TypeSmallInt:
			*v = uint64(int64Val) & 0xFFFF
		case TypeTinyInt:
			*v = uint64(int64Val) & 0xFF
		default:
			*v = uint64(int64Val)
		}
		return nil
	case *int32:
		if int64Val < math.MinInt32 || int64Val > math.MaxInt32 {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int32(int64Val)
		return nil
	case *uint32:
		switch info.Type() {
		case TypeInt:
			*v = uint32(int64Val) & 0xFFFFFFFF
		case TypeSmallInt:
			*v = uint32(int64Val) & 0xFFFF
		case TypeTinyInt:
			*v = uint32(int64Val) & 0xFF
		default:
			if int64Val < 0 || int64Val > math.MaxUint32 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
			}
			*v = uint32(int64Val) & 0xFFFFFFFF
		}
		return nil
	case *int16:
		if int64Val < math.MinInt16 || int64Val > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int16(int64Val)
		return nil
	case *uint16:
		switch info.Type() {
		case TypeSmallInt:
			*v = uint16(int64Val) & 0xFFFF
		case TypeTinyInt:
			*v = uint16(int64Val) & 0xFF
		default:
			if int64Val < 0 || int64Val > math.MaxUint16 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
			}
			*v = uint16(int64Val) & 0xFFFF
		}
		return nil
	case *int8:
		if int64Val < math.MinInt8 || int64Val > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int8(int64Val)
		return nil
	case *uint8:
		if info.Type() != TypeTinyInt && (int64Val < 0 || int64Val > math.MaxUint8) {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = uint8(int64Val) & 0xFF
		return nil
	case *big.Int:
		internal.DecBigInt2C(data, v)
		return nil
	case *string:
		*v = strconv.FormatInt(int64Val, 10)
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()

	switch rv.Type().Kind() {
	case reflect.Int:
		if ^uint(0) == math.MaxUint32 && (int64Val < math.MinInt32 || int64Val > math.MaxInt32) {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Int64:
		rv.SetInt(int64Val)
		return nil
	case reflect.Int32:
		if int64Val < math.MinInt32 || int64Val > math.MaxInt32 {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Int16:
		if int64Val < math.MinInt16 || int64Val > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Int8:
		if int64Val < math.MinInt8 || int64Val > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Uint:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case TypeInt:
			rv.SetUint(unitVal & 0xFFFFFFFF)
		case TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			if ^uint(0) == math.MaxUint32 && (int64Val < 0 || int64Val > math.MaxUint32) {
				return unmarshalErrorf("unmarshal int: value %d out of range for %s", unitVal, rv.Type())
			}
			rv.SetUint(unitVal)
		}
		return nil
	case reflect.Uint64:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case TypeInt:
			rv.SetUint(unitVal & 0xFFFFFFFF)
		case TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			rv.SetUint(unitVal)
		}
		return nil
	case reflect.Uint32:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case TypeInt:
			rv.SetUint(unitVal & 0xFFFFFFFF)
		case TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			if int64Val < 0 || int64Val > math.MaxUint32 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %s", int64Val, rv.Type())
			}
			rv.SetUint(unitVal & 0xFFFFFFFF)
		}
		return nil
	case reflect.Uint16:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			if int64Val < 0 || int64Val > math.MaxUint16 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %s", int64Val, rv.Type())
			}
			rv.SetUint(unitVal & 0xFFFF)
		}
		return nil
	case reflect.Uint8:
		if info.Type() != TypeTinyInt && (int64Val < 0 || int64Val > math.MaxUint8) {
			return unmarshalErrorf("unmarshal int: value %d out of range for %s", int64Val, rv.Type())
		}
		rv.SetUint(uint64(int64Val) & 0xff)
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalBool(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *bool:
		*v = internal.DecBool(data)
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Bool:
		rv.SetBool(internal.DecBool(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalFloat(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float32:
		*v = math.Float32frombits(uint32(internal.DecInt(data)))
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Float32:
		rv.SetFloat(float64(math.Float32frombits(uint32(internal.DecInt(data)))))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDouble(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float64:
		*v = math.Float64frombits(uint64(internal.DecBigInt(data)))
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Float64:
		rv.SetFloat(math.Float64frombits(uint64(internal.DecBigInt(data))))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDecimal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *inf.Dec:
		if len(data) < 4 {
			return unmarshalErrorf("inf.Dec needs at least 4 bytes, while value has only %d", len(data))
		}
		scale := internal.DecInt(data[0:4])
		unscaled := internal.DecBigInt2C(data[4:], nil)
		*v = *inf.NewDecBig(unscaled, inf.Scale(scale))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalTime(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = internal.DecBigInt(data)
		return nil
	case *time.Duration:
		*v = time.Duration(internal.DecBigInt(data))
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Int64:
		rv.SetInt(internal.DecBigInt(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalTimestamp(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = internal.DecBigInt(data)
		return nil
	case *time.Time:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
		x := internal.DecBigInt(data)
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
		rv.SetInt(internal.DecBigInt(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDate(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *time.Time:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
		var origin uint32 = 1 << 31
		var current uint32 = binary.BigEndian.Uint32(data)
		timestamp := (int64(current) - int64(origin)) * internal.MillisecondsInADay
		*v = time.UnixMilli(timestamp).In(time.UTC)
		return nil
	case *string:
		if len(data) == 0 {
			*v = ""
			return nil
		}
		var origin uint32 = 1 << 31
		var current uint32 = binary.BigEndian.Uint32(data)
		timestamp := (int64(current) - int64(origin)) * internal.MillisecondsInADay
		*v = time.UnixMilli(timestamp).In(time.UTC).Format("2006-01-02")
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalDuration(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case internal.UnsetColumn:
		return nil, nil
	case int64:
		return internal.EncVints(0, 0, v), nil
	case time.Duration:
		return internal.EncVints(0, 0, v.Nanoseconds()), nil
	case string:
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, err
		}
		return internal.EncVints(0, 0, d.Nanoseconds()), nil
	case Duration:
		return internal.EncVints(v.Months, v.Days, v.Nanoseconds), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return internal.EncBigInt(rv.Int()), nil
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalDuration(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *Duration:
		if len(data) == 0 {
			*v = Duration{
				Months:      0,
				Days:        0,
				Nanoseconds: 0,
			}
			return nil
		}
		months, days, nanos, err := internal.DecVints(data)
		if err != nil {
			return unmarshalErrorf("failed to unmarshal %s into %T: %s", info, value, err.Error())
		}
		*v = Duration{
			Months:      months,
			Days:        days,
			Nanoseconds: nanos,
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

// TODO: move to internal
// just pass the CollectionType.proto to this method instead of CollectionType
func writeCollectionSize(info CollectionType, n int, buf *bytes.Buffer) error {
	if info.proto > protoVersion2 {
		if n > math.MaxInt32 {
			return marshalErrorf("marshal: collection too large")
		}

		buf.WriteByte(byte(n >> 24))
		buf.WriteByte(byte(n >> 16))
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	} else {
		if n > math.MaxUint16 {
			return marshalErrorf("marshal: collection too large")
		}

		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	}

	return nil
}

func marshalList(info TypeInfo, value interface{}) ([]byte, error) {
	listInfo, ok := info.(CollectionType)
	if !ok {
		return nil, marshalErrorf("marshal: can not marshal non collection type into list")
	}

	if value == nil {
		return nil, nil
	} else if _, ok := value.(internal.UnsetColumn); ok {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	if k == reflect.Slice && rv.IsNil() {
		return nil, nil
	}

	switch k {
	case reflect.Slice, reflect.Array:
		buf := &bytes.Buffer{}
		n := rv.Len()

		if err := writeCollectionSize(listInfo, n, buf); err != nil {
			return nil, err
		}

		for i := 0; i < n; i++ {
			item, err := Marshal(listInfo.Elem, rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			itemLen := len(item)
			// Set the value to null for supported protocols
			if item == nil && listInfo.proto > protoVersion2 {
				itemLen = -1
			}
			if err := writeCollectionSize(listInfo, itemLen, buf); err != nil {
				return nil, err
			}
			buf.Write(item)
		}
		return buf.Bytes(), nil
	case reflect.Map:
		elem := t.Elem()
		if elem.Kind() == reflect.Struct && elem.NumField() == 0 {
			rkeys := rv.MapKeys()
			keys := make([]interface{}, len(rkeys))
			for i := 0; i < len(keys); i++ {
				keys[i] = rkeys[i].Interface()
			}
			return marshalList(listInfo, keys)
		}
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func readCollectionSize(info CollectionType, data []byte) (size, read int, err error) {
	if info.proto > protoVersion2 {
		if len(data) < 4 {
			return 0, 0, unmarshalErrorf("unmarshal list: unexpected eof")
		}
		size = int(int32(data[0])<<24 | int32(data[1])<<16 | int32(data[2])<<8 | int32(data[3]))
		read = 4
	} else {
		if len(data) < 2 {
			return 0, 0, unmarshalErrorf("unmarshal list: unexpected eof")
		}
		size = int(data[0])<<8 | int(data[1])
		read = 2
	}
	return
}

func unmarshalList(info TypeInfo, data []byte, value interface{}) error {
	listInfo, ok := info.(CollectionType)
	if !ok {
		return unmarshalErrorf("unmarshal: can not unmarshal none collection type into list")
	}

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
			if rv.IsNil() {
				return nil
			}
			rv.Set(reflect.Zero(t))
			return nil
		}
		n, p, err := readCollectionSize(listInfo, data)
		if err != nil {
			return err
		}
		data = data[p:]
		if k == reflect.Array {
			if rv.Len() != n {
				return unmarshalErrorf("unmarshal list: array with wrong size")
			}
		} else {
			rv.Set(reflect.MakeSlice(t, n, n))
		}
		for i := 0; i < n; i++ {
			m, p, err := readCollectionSize(listInfo, data)
			if err != nil {
				return err
			}
			data = data[p:]
			// In case m < 0, the value is null, and unmarshalData should be nil.
			var unmarshalData []byte
			if m >= 0 {
				if len(data) < m {
					return unmarshalErrorf("unmarshal list: unexpected eof")
				}
				unmarshalData = data[:m]
				data = data[m:]
			}
			if err := Unmarshal(listInfo.Elem, unmarshalData, rv.Index(i).Addr().Interface()); err != nil {
				return err
			}
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func marshalMap(info TypeInfo, value interface{}) ([]byte, error) {
	mapInfo, ok := info.(CollectionType)
	if !ok {
		return nil, marshalErrorf("marshal: can not marshal none collection type into map")
	}

	if value == nil {
		return nil, nil
	} else if _, ok := value.(internal.UnsetColumn); ok {
		return nil, nil
	}

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

	if err := writeCollectionSize(mapInfo, n, buf); err != nil {
		return nil, err
	}

	keys := rv.MapKeys()
	for _, key := range keys {
		item, err := Marshal(mapInfo.Key, key.Interface())
		if err != nil {
			return nil, err
		}
		itemLen := len(item)
		// Set the key to null for supported protocols
		if item == nil && mapInfo.proto > protoVersion2 {
			itemLen = -1
		}
		if err := writeCollectionSize(mapInfo, itemLen, buf); err != nil {
			return nil, err
		}
		buf.Write(item)

		item, err = Marshal(mapInfo.Elem, rv.MapIndex(key).Interface())
		if err != nil {
			return nil, err
		}
		itemLen = len(item)
		// Set the value to null for supported protocols
		if item == nil && mapInfo.proto > protoVersion2 {
			itemLen = -1
		}
		if err := writeCollectionSize(mapInfo, itemLen, buf); err != nil {
			return nil, err
		}
		buf.Write(item)
	}
	return buf.Bytes(), nil
}

func unmarshalMap(info TypeInfo, data []byte, value interface{}) error {
	mapInfo, ok := info.(CollectionType)
	if !ok {
		return unmarshalErrorf("unmarshal: can not unmarshal none collection type into map")
	}

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
	n, p, err := readCollectionSize(mapInfo, data)
	if err != nil {
		return err
	}
	if n < 0 {
		return unmarshalErrorf("negative map size %d", n)
	}
	rv.Set(reflect.MakeMapWithSize(t, n))
	data = data[p:]
	for i := 0; i < n; i++ {
		m, p, err := readCollectionSize(mapInfo, data)
		if err != nil {
			return err
		}
		data = data[p:]
		key := reflect.New(t.Key())
		// In case m < 0, the key is null, and unmarshalData should be nil.
		var unmarshalData []byte
		if m >= 0 {
			if len(data) < m {
				return unmarshalErrorf("unmarshal map: unexpected eof")
			}
			unmarshalData = data[:m]
			data = data[m:]
		}
		if err := Unmarshal(mapInfo.Key, unmarshalData, key.Interface()); err != nil {
			return err
		}

		m, p, err = readCollectionSize(mapInfo, data)
		if err != nil {
			return err
		}
		data = data[p:]
		val := reflect.New(t.Elem())

		// In case m < 0, the value is null, and unmarshalData should be nil.
		unmarshalData = nil
		if m >= 0 {
			if len(data) < m {
				return unmarshalErrorf("unmarshal map: unexpected eof")
			}
			unmarshalData = data[:m]
			data = data[m:]
		}
		if err := Unmarshal(mapInfo.Elem, unmarshalData, val.Interface()); err != nil {
			return err
		}

		rv.SetMapIndex(key.Elem(), val.Elem())
	}
	return nil
}

func marshalUUID(info TypeInfo, value interface{}) ([]byte, error) {
	switch val := value.(type) {
	case internal.UnsetColumn:
		return nil, nil
	case UUID:
		return val.Bytes(), nil
	case [16]byte:
		return val[:], nil
	case []byte:
		if len(val) != 16 {
			return nil, marshalErrorf("can not marshal []byte %d bytes long into %s, must be exactly 16 bytes long", len(val), info)
		}
		return val, nil
	case string:
		b, err := ParseUUID(val)
		if err != nil {
			return nil, err
		}
		return b[:], nil
	}

	if value == nil {
		return nil, nil
	}

	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

func unmarshalUUID(info TypeInfo, data []byte, value interface{}) error {
	if len(data) == 0 {
		switch v := value.(type) {
		case *string:
			*v = ""
		case *[]byte:
			*v = nil
		case *UUID:
			*v = UUID{}
		default:
			return unmarshalErrorf("can not unmarshal X %s into %T", info, value)
		}

		return nil
	}

	if len(data) != 16 {
		return unmarshalErrorf("unable to parse UUID: UUIDs must be exactly 16 bytes long")
	}

	switch v := value.(type) {
	case *[16]byte:
		copy((*v)[:], data)
		return nil
	case *UUID:
		copy((*v)[:], data)
		return nil
	}

	u, err := UUIDFromBytes(data)
	if err != nil {
		return unmarshalErrorf("unable to parse UUID: %s", err)
	}

	switch v := value.(type) {
	case *string:
		*v = u.String()
		return nil
	case *[]byte:
		*v = u[:]
		return nil
	}
	return unmarshalErrorf("can not unmarshal X %s into %T", info, value)
}

func unmarshalTimeUUID(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *time.Time:
		id, err := UUIDFromBytes(data)
		if err != nil {
			return err
		} else if id.Version() != 1 {
			return unmarshalErrorf("invalid timeuuid")
		}
		*v = id.Time()
		return nil
	default:
		return unmarshalUUID(info, data, value)
	}
}

func unmarshalInet(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *net.IP:
		if x := len(data); !(x == 4 || x == 16) {
			return unmarshalErrorf("cannot unmarshal %s into %T: invalid sized IP: got %d bytes not 4 or 16", info, value, x)
		}
		buf := internal.CopyBytes(data)
		ip := net.IP(buf)
		if v4 := ip.To4(); v4 != nil {
			*v = v4
			return nil
		}
		*v = ip
		return nil
	case *string:
		if len(data) == 0 {
			*v = ""
			return nil
		}
		ip := net.IP(data)
		if v4 := ip.To4(); v4 != nil {
			*v = v4.String()
			return nil
		}
		*v = ip.String()
		return nil
	}
	return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
}

func marshalTuple(info TypeInfo, value interface{}) ([]byte, error) {
	tuple := info.(TupleTypeInfo)
	switch v := value.(type) {
	case internal.UnsetColumn:
		return nil, unmarshalErrorf("Invalid request: UnsetValue is unsupported for tuples")
	case []interface{}:
		if len(v) != len(tuple.Elems) {
			return nil, unmarshalErrorf("cannont marshal tuple: wrong number of elements")
		}

		var buf []byte
		for i, elem := range v {
			if elem == nil {
				buf = internal.AppendInt(buf, int32(-1))
				continue
			}

			data, err := Marshal(tuple.Elems[i], elem)
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = internal.AppendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	}

	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		if v := t.NumField(); v != len(tuple.Elems) {
			return nil, marshalErrorf("can not marshal tuple into struct %v, not enough fields have %d need %d", t, v, len(tuple.Elems))
		}

		var buf []byte
		for i, elem := range tuple.Elems {
			field := rv.Field(i)

			if field.Kind() == reflect.Ptr && field.IsNil() {
				buf = internal.AppendInt(buf, int32(-1))
				continue
			}

			data, err := Marshal(elem, field.Interface())
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = internal.AppendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	case reflect.Slice, reflect.Array:
		size := rv.Len()
		if size != len(tuple.Elems) {
			return nil, marshalErrorf("can not marshal tuple into %v of length %d need %d elements", k, size, len(tuple.Elems))
		}

		var buf []byte
		for i, elem := range tuple.Elems {
			item := rv.Index(i)

			if item.Kind() == reflect.Ptr && item.IsNil() {
				buf = internal.AppendInt(buf, int32(-1))
				continue
			}

			data, err := Marshal(elem, item.Interface())
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = internal.AppendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	}

	return nil, marshalErrorf("cannot marshal %T into %s", value, tuple)
}

// currently only support unmarshal into a list of values, this makes it possible
// to support tuples without changing the query API. In the future this can be extend
// to allow unmarshalling into custom tuple types.
func unmarshalTuple(info TypeInfo, data []byte, value interface{}) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}

	tuple := info.(TupleTypeInfo)
	switch v := value.(type) {
	case []interface{}:
		for i, elem := range tuple.Elems {
			// each element inside data is a [bytes]
			var p []byte
			if len(data) >= 4 {
				p, data = internal.ReadBytes(data)
			}
			err := Unmarshal(elem, p, v[i])
			if err != nil {
				return err
			}
		}

		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}

	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		if v := t.NumField(); v != len(tuple.Elems) {
			return unmarshalErrorf("can not unmarshal tuple into struct %v, not enough fields have %d need %d", t, v, len(tuple.Elems))
		}

		for i, elem := range tuple.Elems {
			var p []byte
			if len(data) >= 4 {
				p, data = internal.ReadBytes(data)
			}

			v, err := elem.NewWithError()
			if err != nil {
				return err
			}
			if err := Unmarshal(elem, p, v); err != nil {
				return err
			}

			switch rv.Field(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					rv.Field(i).Set(reflect.ValueOf(v))
				} else {
					rv.Field(i).Set(reflect.Zero(reflect.TypeOf(v)))
				}
			default:
				rv.Field(i).Set(reflect.ValueOf(v).Elem())
			}
		}

		return nil
	case reflect.Slice, reflect.Array:
		if k == reflect.Array {
			size := rv.Len()
			if size != len(tuple.Elems) {
				return unmarshalErrorf("can not unmarshal tuple into array of length %d need %d elements", size, len(tuple.Elems))
			}
		} else {
			rv.Set(reflect.MakeSlice(t, len(tuple.Elems), len(tuple.Elems)))
		}

		for i, elem := range tuple.Elems {
			var p []byte
			if len(data) >= 4 {
				p, data = internal.ReadBytes(data)
			}

			v, err := elem.NewWithError()
			if err != nil {
				return err
			}
			if err := Unmarshal(elem, p, v); err != nil {
				return err
			}

			switch rv.Index(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					rv.Index(i).Set(reflect.ValueOf(v))
				} else {
					rv.Index(i).Set(reflect.Zero(reflect.TypeOf(v)))
				}
			default:
				rv.Index(i).Set(reflect.ValueOf(v).Elem())
			}
		}

		return nil
	}

	return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
}

// UDTMarshaler is an interface which should be implemented by users wishing to
// handle encoding UDT types to sent to Cassandra. Note: due to current implentations
// methods defined for this interface must be value receivers not pointer receivers.
type UDTMarshaler interface {
	// MarshalUDT will be called for each field in the the UDT returned by Cassandra,
	// the implementor should marshal the type to return by for example calling
	// Marshal.
	MarshalUDT(name string, info TypeInfo) ([]byte, error)
}

// UDTUnmarshaler should be implemented by users wanting to implement custom
// UDT unmarshaling.
type UDTUnmarshaler interface {
	// UnmarshalUDT will be called for each field in the UDT return by Cassandra,
	// the implementor should unmarshal the data into the value of their chosing,
	// for example by calling Unmarshal.
	UnmarshalUDT(name string, info TypeInfo, data []byte) error
}

func marshalUDT(info TypeInfo, value interface{}) ([]byte, error) {
	udt := info.(UDTTypeInfo)

	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case internal.UnsetColumn:
		return nil, unmarshalErrorf("invalid request: UnsetValue is unsupported for user defined types")
	case UDTMarshaler:
		var buf []byte
		for _, e := range udt.Elements {
			data, err := v.MarshalUDT(e.Name, e.Type)
			if err != nil {
				return nil, err
			}

			buf = internal.AppendBytes(buf, data)
		}

		return buf, nil
	case map[string]interface{}:
		var buf []byte
		for _, e := range udt.Elements {
			val, ok := v[e.Name]

			var data []byte

			if ok {
				var err error
				data, err = Marshal(e.Type, val)
				if err != nil {
					return nil, err
				}
			}

			buf = internal.AppendBytes(buf, data)
		}

		return buf, nil
	}

	k := reflect.ValueOf(value)
	if k.Kind() == reflect.Ptr {
		if k.IsNil() {
			return nil, marshalErrorf("cannot marshal %T into %s", value, info)
		}
		k = k.Elem()
	}

	if k.Kind() != reflect.Struct || !k.IsValid() {
		return nil, marshalErrorf("cannot marshal %T into %s", value, info)
	}

	fields := make(map[string]reflect.Value)
	t := reflect.TypeOf(value)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		if tag := sf.Tag.Get("cql"); tag != "" {
			fields[tag] = k.Field(i)
		}
	}

	var buf []byte
	for _, e := range udt.Elements {
		f, ok := fields[e.Name]
		if !ok {
			f = k.FieldByName(e.Name)
		}

		var data []byte
		if f.IsValid() && f.CanInterface() {
			var err error
			data, err = Marshal(e.Type, f.Interface())
			if err != nil {
				return nil, err
			}
		}

		buf = internal.AppendBytes(buf, data)
	}

	return buf, nil
}

func unmarshalUDT(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case UDTUnmarshaler:
		udt := info.(UDTTypeInfo)

		for id, e := range udt.Elements {
			if len(data) == 0 {
				return nil
			}
			if len(data) < 4 {
				return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", info, id, e.Name)
			}

			var p []byte
			p, data = internal.ReadBytes(data)
			if err := v.UnmarshalUDT(e.Name, e.Type, p); err != nil {
				return err
			}
		}

		return nil
	case *map[string]interface{}:
		udt := info.(UDTTypeInfo)

		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Ptr {
			return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
		}

		rv = rv.Elem()
		t := rv.Type()
		if t.Kind() != reflect.Map {
			return unmarshalErrorf("can not unmarshal %s into %T", info, value)
		} else if data == nil {
			rv.Set(reflect.Zero(t))
			return nil
		}

		rv.Set(reflect.MakeMap(t))
		m := *v

		for id, e := range udt.Elements {
			if len(data) == 0 {
				return nil
			}
			if len(data) < 4 {
				return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", info, id, e.Name)
			}

			valType, err := goType(e.Type)
			if err != nil {
				return unmarshalErrorf("can not unmarshal %s: %v", info, err)
			}

			val := reflect.New(valType)

			var p []byte
			p, data = internal.ReadBytes(data)

			if err := Unmarshal(e.Type, p, val.Interface()); err != nil {
				return err
			}

			m[e.Name] = val.Elem().Interface()
		}

		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	k := rv.Elem()
	if k.Kind() != reflect.Struct || !k.IsValid() {
		return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
	}

	if len(data) == 0 {
		if k.CanSet() {
			k.Set(reflect.Zero(k.Type()))
		}

		return nil
	}

	t := k.Type()
	fields := make(map[string]reflect.Value, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		if tag := sf.Tag.Get("cql"); tag != "" {
			fields[tag] = k.Field(i)
		}
	}

	udt := info.(UDTTypeInfo)
	for id, e := range udt.Elements {
		if len(data) == 0 {
			return nil
		}
		if len(data) < 4 {
			// UDT def does not match the column value
			return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", info, id, e.Name)
		}

		var p []byte
		p, data = internal.ReadBytes(data)

		f, ok := fields[e.Name]
		if !ok {
			f = k.FieldByName(e.Name)
			if f == internal.EmptyValue {
				// skip fields which exist in the UDT but not in
				// the struct passed in
				continue
			}
		}

		if !f.IsValid() || !f.CanAddr() {
			return unmarshalErrorf("cannot unmarshal %s into %T: field %v is not valid", info, value, e.Name)
		}

		fk := f.Addr().Interface()
		if err := Unmarshal(e.Type, p, fk); err != nil {
			return err
		}
	}

	return nil
}

// TypeInfo describes a Cassandra specific data type.
type TypeInfo interface {
	Type() Type
	Version() byte
	Custom() string

	// New creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, New panics.
	//
	// Deprecated: Use NewWithError instead.
	New() interface{}

	// NewWithError creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, NewWithError returns an error.
	NewWithError() (interface{}, error)
}

type NativeType struct {
	proto  byte
	typ    Type
	custom string // only used for TypeCustom
}

func NewNativeType(proto byte, typ Type, custom string) NativeType {
	return NativeType{proto, typ, custom}
}

func (t NativeType) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t NativeType) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (s NativeType) Type() Type {
	return s.typ
}

func (s NativeType) Version() byte {
	return s.proto
}

func (s NativeType) Custom() string {
	return s.custom
}

func (s NativeType) String() string {
	switch s.typ {
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", s.typ, s.custom)
	default:
		return s.typ.String()
	}
}

type CollectionType struct {
	NativeType
	Key  TypeInfo // only used for TypeMap
	Elem TypeInfo // only used for TypeMap, TypeList and TypeSet
}

func (t CollectionType) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t CollectionType) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (c CollectionType) String() string {
	switch c.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", c.typ, c.Key, c.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", c.typ, c.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", c.typ, c.custom)
	default:
		return c.typ.String()
	}
}

type TupleTypeInfo struct {
	NativeType
	Elems []TypeInfo
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

func (t TupleTypeInfo) NewWithError() (interface{}, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t TupleTypeInfo) New() interface{} {
	val, err := t.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

type UDTField struct {
	Name string
	Type TypeInfo
}

type UDTTypeInfo struct {
	NativeType
	KeySpace string
	Name     string
	Elements []UDTField
}

func (u UDTTypeInfo) NewWithError() (interface{}, error) {
	typ, err := goType(u)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (u UDTTypeInfo) New() interface{} {
	val, err := u.NewWithError()
	if err != nil {
		panic(err.Error())
	}
	return val
}

func (u UDTTypeInfo) String() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%s.%s{", u.KeySpace, u.Name)
	first := true
	for _, e := range u.Elements {
		if !first {
			fmt.Fprint(buf, ",")
		} else {
			first = false
		}

		fmt.Fprintf(buf, "%s=%v", e.Name, e.Type)
	}
	fmt.Fprint(buf, "}")

	return buf.String()
}

// String returns a human readable name for the Cassandra datatype
// described by t.
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
	TypeText      Type = 0x000A
	TypeTimestamp Type = 0x000B
	TypeUUID      Type = 0x000C
	TypeVarchar   Type = 0x000D
	TypeVarint    Type = 0x000E
	TypeTimeUUID  Type = 0x000F
	TypeInet      Type = 0x0010
	TypeDate      Type = 0x0011
	TypeTime      Type = 0x0012
	TypeSmallInt  Type = 0x0013
	TypeTinyInt   Type = 0x0014
	TypeDuration  Type = 0x0015
	TypeList      Type = 0x0020
	TypeMap       Type = 0x0021
	TypeSet       Type = 0x0022
	TypeUDT       Type = 0x0030
	TypeTuple     Type = 0x0031
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
	case TypeText:
		return "text"
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
	case TypeDate:
		return "date"
	case TypeDuration:
		return "duration"
	case TypeTime:
		return "time"
	case TypeSmallInt:
		return "smallint"
	case TypeTinyInt:
		return "tinyint"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	case TypeVarint:
		return "varint"
	case TypeTuple:
		return "tuple"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
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
