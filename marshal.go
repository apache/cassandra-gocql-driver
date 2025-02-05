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
	"math"
	"math/big"
	"math/bits"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/inf.v0"
)

var (
	bigOne     = big.NewInt(1)
	emptyValue reflect.Value
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
//	bigint, counter             | big.Int            | according to cassandra bigint specification the big.Int value limited to int64 size(an eight-byte two's complement integer.)
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
//
// The marshal/unmarshal error provides a list of supported types when an unsupported type is attempted.
func Marshal(info TypeInfo, value interface{}) ([]byte, error) {
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

	// detect protocol 2 UDT
	if nt, ok := info.(NativeType); ok && strings.HasPrefix(nt.Custom(), "org.apache.cassandra.db.marshal.UserType") && nt.Version() < 3 {
		return nil, ErrorUDTUnavailable
	}

	// this is ~10% faster than fastRegisteredTypeLookup
	switch info.Type() {
	case TypeAscii:
		return asciiRegisteredType.Marshal(info, value)
	case TypeBigInt:
		return bigIntRegisteredType.Marshal(info, value)
	case TypeBlob:
		return blobRegisteredType.Marshal(info, value)
	case TypeBoolean:
		return booleanRegisteredType.Marshal(info, value)
	case TypeCounter:
		return counterRegisteredType.Marshal(info, value)
	case TypeDate:
		return dateRegisteredType.Marshal(info, value)
	case TypeDecimal:
		return decimalRegisteredType.Marshal(info, value)
	case TypeDouble:
		return doubleRegisteredType.Marshal(info, value)
	case TypeDuration:
		return durationRegisteredType.Marshal(info, value)
	case TypeFloat:
		return floatRegisteredType.Marshal(info, value)
	case TypeInet:
		return inetRegisteredType.Marshal(info, value)
	case TypeInt:
		return intRegisteredType.Marshal(info, value)
	case TypeSmallInt:
		return smallintRegisteredType.Marshal(info, value)
	case TypeText:
		return textRegisteredType.Marshal(info, value)
	case TypeTime:
		return timeRegisteredType.Marshal(info, value)
	case TypeTimestamp:
		return timestampRegisteredType.Marshal(info, value)
	case TypeTimeUUID:
		return timeUUIDRegisteredType.Marshal(info, value)
	case TypeTinyInt:
		return tinyIntRegisteredType.Marshal(info, value)
	case TypeUUID:
		return uuidRegisteredType.Marshal(info, value)
	case TypeVarchar:
		return varcharRegisteredType.Marshal(info, value)
	case TypeVarint:
		return varintRegisteredType.Marshal(info, value)
	case TypeList:
		return listRegisteredType.Marshal(info, value)
	case TypeMap:
		return mapRegisteredType.Marshal(info, value)
	case TypeSet:
		return setRegisteredType.Marshal(info, value)
	case TypeTuple:
		return tupleRegisteredType.Marshal(info, value)
	case TypeUDT:
		return udtRegisteredType.Marshal(info, value)
	case TypeCustom:
		return customRegisteredType.Marshal(info, value)
	default:
		rt, ok := registeredTypes[info.Type()]
		if !ok {
			return nil, fmt.Errorf("can not marshal %T into %s", value, info)
		}
		return rt.Marshal(info, value)
	}
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

	// detect protocol 2 UDT
	if nt, ok := info.(NativeType); ok && strings.HasPrefix(nt.Custom(), "org.apache.cassandra.db.marshal.UserType") && nt.Version() < 3 {
		return ErrorUDTUnavailable
	}

	// this is ~10% faster than fastRegisteredTypeLookup
	switch info.Type() {
	case TypeAscii:
		return asciiRegisteredType.Unmarshal(info, data, value)
	case TypeBigInt:
		return bigIntRegisteredType.Unmarshal(info, data, value)
	case TypeBlob:
		return blobRegisteredType.Unmarshal(info, data, value)
	case TypeBoolean:
		return booleanRegisteredType.Unmarshal(info, data, value)
	case TypeCounter:
		return counterRegisteredType.Unmarshal(info, data, value)
	case TypeDate:
		return dateRegisteredType.Unmarshal(info, data, value)
	case TypeDecimal:
		return decimalRegisteredType.Unmarshal(info, data, value)
	case TypeDouble:
		return doubleRegisteredType.Unmarshal(info, data, value)
	case TypeDuration:
		return durationRegisteredType.Unmarshal(info, data, value)
	case TypeFloat:
		return floatRegisteredType.Unmarshal(info, data, value)
	case TypeInet:
		return inetRegisteredType.Unmarshal(info, data, value)
	case TypeInt:
		return intRegisteredType.Unmarshal(info, data, value)
	case TypeSmallInt:
		return smallintRegisteredType.Unmarshal(info, data, value)
	case TypeText:
		return textRegisteredType.Unmarshal(info, data, value)
	case TypeTime:
		return timeRegisteredType.Unmarshal(info, data, value)
	case TypeTimestamp:
		return timestampRegisteredType.Unmarshal(info, data, value)
	case TypeTimeUUID:
		return timeUUIDRegisteredType.Unmarshal(info, data, value)
	case TypeTinyInt:
		return tinyIntRegisteredType.Unmarshal(info, data, value)
	case TypeUUID:
		return uuidRegisteredType.Unmarshal(info, data, value)
	case TypeVarchar:
		return varcharRegisteredType.Unmarshal(info, data, value)
	case TypeVarint:
		return varintRegisteredType.Unmarshal(info, data, value)
	case TypeList:
		return listRegisteredType.Unmarshal(info, data, value)
	case TypeMap:
		return mapRegisteredType.Unmarshal(info, data, value)
	case TypeSet:
		return setRegisteredType.Unmarshal(info, data, value)
	case TypeTuple:
		return tupleRegisteredType.Unmarshal(info, data, value)
	case TypeUDT:
		return udtRegisteredType.Unmarshal(info, data, value)
	case TypeCustom:
		return customRegisteredType.Unmarshal(info, data, value)
	default:
		rt, ok := registeredTypes[info.Type()]
		if !ok {
			return fmt.Errorf("unmarshal: can not unmarshal unknown type %s", info)
		}
		return rt.Unmarshal(info, data, value)
	}
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

type varcharLikeCQLType struct {
	typ Type
}

// varcharLikeCQLType doesn't require any params
func (varcharLikeCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (t varcharLikeCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for varchar-like type, got %d", len(params)))
	}
	return t.typ
}

// TypeInfoFromString returns the type itself.
func (t varcharLikeCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for varchar-like type, got %s", name))
	}
	return t.typ
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (varcharLikeCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	switch {
	case k == reflect.String:
		return []byte(rv.String()), nil
	case k == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return rv.Bytes(), nil
	}
	return nil, marshalErrorf("can not marshal %T into %s. Accepted types: Marshaler, string, []byte, UnsetValue.", value, info)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (varcharLikeCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
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
	case *interface{}:
		if info.Type() == TypeBlob {
			if data != nil {
				*v = make([]byte, len(data))
				copy((*v).([]byte), data)
			} else {
				*v = []byte(nil)
			}
			return nil
		}
		*v = string(data)
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
	return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: Unmarshaler, *string, *[]byte", info, value)
}

type smallIntCQLType struct{}

// smallIntCQLType doesn't require any params
func (smallIntCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (smallIntCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for small int type, got %d", len(params)))
	}
	return TypeSmallInt
}

// TypeInfoFromString returns the type itself.
func (smallIntCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for small int type, got %s", name))
	}
	return TypeSmallInt
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (smallIntCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int16:
		return encShort(v), nil
	case uint16:
		return encShort(int16(v)), nil
	case int8:
		return encShort(int16(v)), nil
	case uint8:
		return encShort(int16(v)), nil
	case int:
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case int32:
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case int64:
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case uint:
		if v > math.MaxUint16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case uint32:
		if v > math.MaxUint16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case uint64:
		if v > math.MaxUint16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case string:
		n, err := strconv.ParseInt(v, 10, 16)
		if err != nil {
			return nil, marshalErrorf("can not marshal %T into %s: %v", value, info, err)
		}
		return encShort(int16(n)), nil
	}

	if value == nil {
		return nil, nil
	}

	switch rv := reflect.ValueOf(value); rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxUint16 {
			return nil, marshalErrorf("marshal smallint: value %d out of range", v)
		}
		return encShort(int16(v)), nil
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
	}

	return nil, marshalErrorf("can not marshal %T into smallint. Accepted types: Marshaler, int16, uint16, int8, uint8, int, uint, int32, uint32, int64, uint64, string, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (smallIntCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	if iptr, ok := value.(*interface{}); ok && iptr != nil {
		var v int16
		err := unmarshalIntlike(info, int64(decShort(data)), data, &v)
		if err != nil {
			return err
		}
		*iptr = v
		return nil
	}
	return unmarshalIntlike(info, int64(decShort(data)), data, value)
}

type tinyIntCQLType struct{}

// tinyIntCQLType doesn't require any params
func (tinyIntCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (tinyIntCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for tinyInt type, got %d", len(params)))
	}
	return TypeTinyInt
}

// TypeInfoFromString returns the type itself.
func (tinyIntCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for tinyInt type, got %s", name))
	}
	return TypeTinyInt
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (tinyIntCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int8:
		return []byte{byte(v)}, nil
	case uint8:
		return []byte{byte(v)}, nil
	case int16:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint16:
		if v > math.MaxUint8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case int:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case int32:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case int64:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint:
		if v > math.MaxUint8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint32:
		if v > math.MaxUint8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint64:
		if v > math.MaxUint8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 8)
		if err != nil {
			return nil, marshalErrorf("can not marshal %T into %s: %v", value, info, err)
		}
		return []byte{byte(n)}, nil
	}

	if value == nil {
		return nil, nil
	}

	switch rv := reflect.ValueOf(value); rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxUint8 {
			return nil, marshalErrorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
	}

	return nil, marshalErrorf("can not marshal %T into tinyint. Accepted types: Marshaler, int8, uint8, int16, uint16, int, uint, int32, uint32, int64, uint64, string, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (tinyIntCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	if iptr, ok := value.(*interface{}); ok && iptr != nil {
		var v int8
		*iptr = v
		value = &v
	}
	return unmarshalIntlike(info, int64(decTiny(data)), data, value)
}

type intCQLType struct{}

// intCQLType doesn't require any params
func (intCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (intCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for int type, got %d", len(params)))
	}
	return TypeInt
}

// TypeInfoFromString returns the type itself.
func (intCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for int type, got %s", name))
	}
	return TypeInt
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (intCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case uint:
		if v > math.MaxUint32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case int64:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case uint64:
		if v > math.MaxUint32 {
			return nil, marshalErrorf("marshal int: value %d out of range", v)
		}
		return encInt(int32(v)), nil
	case int32:
		return encInt(v), nil
	case uint32:
		return encInt(int32(v)), nil
	case int16:
		return encInt(int32(v)), nil
	case uint16:
		return encInt(int32(v)), nil
	case int8:
		return encInt(int32(v)), nil
	case uint8:
		return encInt(int32(v)), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, marshalErrorf("can not marshal string to int: %s", err)
		}
		return encInt(int32(i)), nil
	}

	if value == nil {
		return nil, nil
	}

	switch rv := reflect.ValueOf(value); rv.Type().Kind() {
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
		if rv.IsNil() {
			return nil, nil
		}
	}

	return nil, marshalErrorf("can not marshal %T into int. Accepted types: Marshaler, int8, uint8, int16, uint16, int, uint, int32, uint32, int64, uint64, string, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (intCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	if iptr, ok := value.(*interface{}); ok && iptr != nil {
		var v int
		err := unmarshalIntlike(info, int64(decInt(data)), data, &v)
		if err != nil {
			return err
		}
		*iptr = v
		return nil
	}
	return unmarshalIntlike(info, int64(decInt(data)), data, value)
}

func encInt(x int32) []byte {
	return []byte{byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
}

func decInt(x []byte) int32 {
	if len(x) != 4 {
		return 0
	}
	return int32(x[0])<<24 | int32(x[1])<<16 | int32(x[2])<<8 | int32(x[3])
}

func encShort(x int16) []byte {
	p := make([]byte, 2)
	p[0] = byte(x >> 8)
	p[1] = byte(x)
	return p
}

func decShort(p []byte) int16 {
	if len(p) != 2 {
		return 0
	}
	return int16(p[0])<<8 | int16(p[1])
}

func decTiny(p []byte) int8 {
	if len(p) != 1 {
		return 0
	}
	return int8(p[0])
}

type bigIntLikeCQLType struct {
	typ Type
}

// bigIntLikeCQLType doesn't require any params
func (bigIntLikeCQLType) Params(int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (t bigIntLikeCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for bigint type, got %d", len(params)))
	}
	return t.typ
}

// TypeInfoFromString returns the type itself.
func (t bigIntLikeCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for bigint type, got %s", name))
	}
	return t.typ
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (bigIntLikeCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
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
	case big.Int:
		if !v.IsInt64() {
			return nil, marshalErrorf("marshal bigint: value %v out of range", &v)
		}
		return encBigInt(v.Int64()), nil
	case string:
		i, err := strconv.ParseInt(value.(string), 10, 64)
		if err != nil {
			return nil, marshalErrorf("can not marshal string to bigint: %s", err)
		}
		return encBigInt(i), nil
	}

	if value == nil {
		return nil, nil
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
	}
	return nil, marshalErrorf("can not marshal %T into %s. Accepted types: big.Int, Marshaler, int8, uint8, int16, uint16, int, uint, int32, uint32, int64, uint64, string, UnsetValue.", value, info)
}

func encBigInt(x int64) []byte {
	return []byte{byte(x >> 56), byte(x >> 48), byte(x >> 40), byte(x >> 32),
		byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
}

func bytesToInt64(data []byte) (ret int64) {
	for i := range data {
		ret |= int64(data[i]) << (8 * uint(len(data)-i-1))
	}
	return ret
}

func bytesToUint64(data []byte) (ret uint64) {
	for i := range data {
		ret |= uint64(data[i]) << (8 * uint(len(data)-i-1))
	}
	return ret
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (bigIntLikeCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	if iptr, ok := value.(*interface{}); ok && iptr != nil {
		var v int64
		err := unmarshalIntlike(info, decBigInt(data), data, &v)
		if err != nil {
			return err
		}
		*iptr = v
		return nil
	}
	return unmarshalIntlike(info, decBigInt(data), data, value)
}

type varintCQLType struct{}

// varintCQLType doesn't require any params
func (varintCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (varintCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for varint type, got %d", len(params)))
	}
	return TypeVarint
}

// TypeInfoFromString returns the type itself.
func (varintCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for varint type, got %s", name))
	}
	return TypeVarint
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (varintCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	var (
		retBytes []byte
		err      error
	)

	switch v := value.(type) {
	case unsetColumn:
		return nil, nil
	case uint64:
		if v > uint64(math.MaxInt64) {
			retBytes = make([]byte, 9)
			binary.BigEndian.PutUint64(retBytes[1:], v)
		} else {
			retBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(retBytes, v)
		}
	case big.Int:
		retBytes = encBigInt2C(&v)
	default:
		retBytes, err = (bigIntLikeCQLType{}).Marshal(info, value)
	}

	if err == nil {
		// trim down to most significant byte
		i := 0
		for ; i < len(retBytes)-1; i++ {
			b0 := retBytes[i]
			if b0 != 0 && b0 != 0xFF {
				break
			}

			b1 := retBytes[i+1]
			if b0 == 0 && b1 != 0 {
				if b1&0x80 == 0 {
					i++
				}
				break
			}

			if b0 == 0xFF && b1 != 0xFF {
				if b1&0x80 > 0 {
					i++
				}
				break
			}
		}
		retBytes = retBytes[i:]
	}

	return retBytes, err
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (varintCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case *big.Int:
		return unmarshalIntlike(info, 0, data, value)
	case *uint64:
		if len(data) == 9 && data[0] == 0 {
			*v = bytesToUint64(data[1:])
			return nil
		}
	case *interface{}:
		var bi big.Int
		if err := unmarshalIntlike(info, 0, data, &bi); err != nil {
			return err
		}
		*v = &bi
		return nil
	}

	if len(data) > 8 {
		return unmarshalErrorf("unmarshal int: varint value %v out of range for %T (use big.Int)", data, value)
	}

	int64Val := bytesToInt64(data)
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
		decBigInt2C(data, v)
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
	return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: big.Int, Marshaler, int8, uint8, int16, uint16, int, uint, int32, uint32, int64, uint64, string, *interface{}.", info, value)
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

type booleanCQLType struct{}

// BoolCQLType doesn't require any params
func (booleanCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (booleanCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for boolean type, got %d", len(params)))
	}
	return TypeBoolean
}

// TypeInfoFromString returns the type itself.
func (booleanCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for boolean type, got %s", name))
	}
	return TypeBoolean
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (booleanCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case bool:
		return encBool(v), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Bool:
		return encBool(rv.Bool()), nil
	}
	return nil, marshalErrorf("can not marshal %T into boolean. Accepted types: Marshaler, bool, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (booleanCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *bool:
		*v = decBool(data)
		return nil
	case *interface{}:
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
	return unmarshalErrorf("can not unmarshal boolean into %T. Accepted types: Unmarshaler, *bool, *interface{}.", value)
}

func encBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func decBool(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	return v[0] != 0
}

type floatCQLType struct{}

// floatCQLType doesn't require any params
func (floatCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (floatCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for float type, got %d", len(params)))
	}
	return TypeFloat
}

// TypeInfoFromString returns the type itself.
func (floatCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for float type, got %s", name))
	}
	return TypeFloat
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (floatCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case float32:
		return encInt(int32(math.Float32bits(v))), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Float32:
		return encInt(int32(math.Float32bits(float32(rv.Float())))), nil
	}
	return nil, marshalErrorf("can not marshal %T into float. Accepted types: Marshaler, float32, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (floatCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float32:
		*v = math.Float32frombits(uint32(decInt(data)))
		return nil
	case *interface{}:
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
	return unmarshalErrorf("can not unmarshal float into %T. Accepted types: Unmarshaler, *float32, *interface{}, UnsetValue.", value)
}

type doubleCQLType struct{}

// doubleCQLType doesn't require any params
func (doubleCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (doubleCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for double type, got %d", len(params)))
	}
	return TypeDouble
}

// TypeInfoFromString returns the type itself.
func (doubleCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for double type, got %s", name))
	}
	return TypeDouble
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (doubleCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case float64:
		return encBigInt(int64(math.Float64bits(v))), nil
	}
	if value == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Float64:
		return encBigInt(int64(math.Float64bits(rv.Float()))), nil
	}
	return nil, marshalErrorf("can not marshal %T into double. Accepted types: Marshaler, float64, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (doubleCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float64:
		*v = math.Float64frombits(uint64(decBigInt(data)))
		return nil
	case *interface{}:
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
	return unmarshalErrorf("can not unmarshal double into %T. Accepted types: Unmarshaler, *float64, *interface{}.", value)
}

type decimalCQLType struct{}

// decimalCQLType doesn't require any params
func (decimalCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (decimalCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for decimal type, got %d", len(params)))
	}
	return TypeDecimal
}

// TypeInfoFromString returns the type itself.
func (decimalCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for decimal type, got %s", name))
	}
	return TypeDecimal
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (decimalCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case inf.Dec:
		unscaled := encBigInt2C(v.UnscaledBig())
		if unscaled == nil {
			return nil, marshalErrorf("can not marshal %T into %s", value, info)
		}

		buf := make([]byte, 4+len(unscaled))
		copy(buf[0:4], encInt(int32(v.Scale())))
		copy(buf[4:], unscaled)
		return buf, nil
	}
	return nil, marshalErrorf("can not marshal %T into decimal. Accepted types: Marshaler, inf.Dec, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (decimalCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *inf.Dec:
		if len(data) < 4 {
			return unmarshalErrorf("inf.Dec needs at least 4 bytes, while value has only %d", len(data))
		}
		scale := decInt(data[0:4])
		unscaled := decBigInt2C(data[4:], nil)
		*v = *inf.NewDecBig(unscaled, inf.Scale(scale))
		return nil
	case *interface{}:
		if len(data) < 4 {
			return unmarshalErrorf("inf.Dec needs at least 4 bytes, while value has only %d", len(data))
		}
		scale := decInt(data[0:4])
		unscaled := decBigInt2C(data[4:], nil)
		*v = inf.NewDecBig(unscaled, inf.Scale(scale))
		return nil
	}
	return unmarshalErrorf("can not unmarshal decimal into %T. Accepted types: Unmarshaler, *inf.Dec, *interface{}.", value)
}

// decBigInt2C sets the value of n to the big-endian two's complement
// value stored in the given data. If data[0]&80 != 0, the number
// is negative. If data is empty, the result will be 0.
func decBigInt2C(data []byte, n *big.Int) *big.Int {
	if n == nil {
		n = new(big.Int)
	}
	n.SetBytes(data)
	if len(data) > 0 && data[0]&0x80 > 0 {
		n.Sub(n, new(big.Int).Lsh(bigOne, uint(len(data))*8))
	}
	return n
}

// encBigInt2C returns the big-endian two's complement
// form of n.
func encBigInt2C(n *big.Int) []byte {
	switch n.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b
	case -1:
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(bigOne, length)).Bytes()
		// When the most significant bit is on a byte
		// boundary, we can get some extra significant
		// bits, so strip them off when that happens.
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b
	}
	return nil
}

type timestampCQLType struct{}

// timestampCQLType doesn't require any params
func (timestampCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (timestampCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for timestamp type, got %d", len(params)))
	}
	return TypeTimestamp
}

// TypeInfoFromString returns the type itself.
func (timestampCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for timestamp type, got %s", name))
	}
	return TypeTimestamp
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (timestampCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int64:
		return encBigInt(v), nil
	case time.Time:
		if v.IsZero() {
			return []byte{}, nil
		}
		x := int64(v.UTC().Unix()*1e3) + int64(v.UTC().Nanosecond()/1e6)
		return encBigInt(x), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return encBigInt(rv.Int()), nil
	}
	return nil, marshalErrorf("can not marshal %T into timestamp. Accepted types: Marshaler, int64, time.Time, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (timestampCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = decBigInt(data)
		return nil
	case *time.Time:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
		x := decBigInt(data)
		sec := x / 1000
		nsec := (x - sec*1000) * 1000000
		*v = time.Unix(sec, nsec).In(time.UTC)
		return nil
	case *interface{}:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
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
	return unmarshalErrorf("can not unmarshal timestamp into %T. Accepted types: Unmarshaler, *int64, *time.Time, *interface{}.", value)
}

type timeCQLType struct{}

// timeCQLType doesn't require any params
func (timeCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (timeCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for time type, got %d", len(params)))
	}
	return TypeTime
}

// TypeInfoFromString returns the type itself.
func (timeCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for time type, got %s", name))
	}
	return TypeTime
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (timeCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int64:
		return encBigInt(v), nil
	case time.Duration:
		return encBigInt(v.Nanoseconds()), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return encBigInt(rv.Int()), nil
	}
	return nil, marshalErrorf("can not marshal %T into time. Accepted types: Marshaler, int64, time.Duration, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (timeCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = decBigInt(data)
		return nil
	case *time.Duration:
		*v = time.Duration(decBigInt(data))
		return nil
	case *interface{}:
		*v = time.Duration(decBigInt(data))
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
	return unmarshalErrorf("can not unmarshal time into %T. Accepted types: Unmarshaler, *int64, *time.Duration, *interface{}.", value)
}

type dateCQLType struct{}

// dateCQLType doesn't require any params
func (dateCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (dateCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for date type, got %d", len(params)))
	}
	return TypeDate
}

// TypeInfoFromString returns the type itself.
func (dateCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for date type, got %s", name))
	}
	return TypeDate
}

const millisecondsInADay int64 = 24 * 60 * 60 * 1000

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (dateCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	var timestamp int64
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int64:
		timestamp = v
		x := timestamp/millisecondsInADay + int64(1<<31)
		return encInt(int32(x)), nil
	case time.Time:
		if v.IsZero() {
			return []byte{}, nil
		}
		timestamp = int64(v.UTC().Unix()*1e3) + int64(v.UTC().Nanosecond()/1e6)
		x := timestamp/millisecondsInADay + int64(1<<31)
		return encInt(int32(x)), nil
	case *time.Time:
		if v.IsZero() {
			return []byte{}, nil
		}
		timestamp = int64(v.UTC().Unix()*1e3) + int64(v.UTC().Nanosecond()/1e6)
		x := timestamp/millisecondsInADay + int64(1<<31)
		return encInt(int32(x)), nil
	case string:
		if v == "" {
			return []byte{}, nil
		}
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return nil, marshalErrorf("can not marshal %T into %s, date layout must be '2006-01-02'", value, info)
		}
		timestamp = int64(t.UTC().Unix()*1e3) + int64(t.UTC().Nanosecond()/1e6)
		x := timestamp/millisecondsInADay + int64(1<<31)
		return encInt(int32(x)), nil
	}

	if value == nil {
		return nil, nil
	}
	return nil, marshalErrorf("can not marshal %T into date. Accepted types: Marshaler, int64, time.Time, *time.Time, string, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (dateCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
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
		timestamp := (int64(current) - int64(origin)) * millisecondsInADay
		*v = time.UnixMilli(timestamp).In(time.UTC)
		return nil
	case *interface{}:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
		var origin uint32 = 1 << 31
		var current uint32 = binary.BigEndian.Uint32(data)
		timestamp := (int64(current) - int64(origin)) * millisecondsInADay
		*v = time.UnixMilli(timestamp).In(time.UTC)
		return nil
	case *string:
		if len(data) == 0 {
			*v = ""
			return nil
		}
		var origin uint32 = 1 << 31
		var current uint32 = binary.BigEndian.Uint32(data)
		timestamp := (int64(current) - int64(origin)) * millisecondsInADay
		*v = time.UnixMilli(timestamp).In(time.UTC).Format("2006-01-02")
		return nil
	}
	return unmarshalErrorf("can not unmarshal date into %T. Accepted types: Unmarshaler, *time.Time, *interface{}, *string.", value)
}

type durationCQLType struct{}

// durationCQLType doesn't require any params
func (durationCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (durationCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for duration type, got %d", len(params)))
	}
	return TypeDuration
}

// TypeInfoFromString returns the type itself.
func (durationCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for duration type, got %s", name))
	}
	return TypeDuration
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (durationCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, nil
	case int64:
		return encVints(0, 0, v), nil
	case time.Duration:
		return encVints(0, 0, v.Nanoseconds()), nil
	case string:
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, err
		}
		return encVints(0, 0, d.Nanoseconds()), nil
	case Duration:
		return encVints(v.Months, v.Days, v.Nanoseconds), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return encBigInt(rv.Int()), nil
	}
	return nil, marshalErrorf("can not marshal %T into duration. Accepted types: Marshaler, int64, time.Duration, string, Duration, UnsetValue.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (durationCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
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
		months, days, nanos, err := decVints(data)
		if err != nil {
			return unmarshalErrorf("failed to unmarshal duration into %T: %s", value, err.Error())
		}
		*v = Duration{
			Months:      months,
			Days:        days,
			Nanoseconds: nanos,
		}
		return nil
	case *interface{}:
		if len(data) == 0 {
			*v = Duration{
				Months:      0,
				Days:        0,
				Nanoseconds: 0,
			}
			return nil
		}
		months, days, nanos, err := decVints(data)
		if err != nil {
			return unmarshalErrorf("failed to unmarshal duration into %T: %s", value, err.Error())
		}
		*v = Duration{
			Months:      months,
			Days:        days,
			Nanoseconds: nanos,
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal duration into %T. Accepted types: Unmarshaler, *Duration, *interface{}.", value)
}

func decVints(data []byte) (int32, int32, int64, error) {
	month, i, err := decVint(data, 0)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to extract month: %s", err.Error())
	}
	days, i, err := decVint(data, i)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to extract days: %s", err.Error())
	}
	nanos, _, err := decVint(data, i)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to extract nanoseconds: %s", err.Error())
	}
	return int32(month), int32(days), nanos, err
}

func decVint(data []byte, start int) (int64, int, error) {
	if len(data) <= start {
		return 0, 0, errors.New("unexpected eof")
	}
	firstByte := data[start]
	if firstByte&0x80 == 0 {
		return decIntZigZag(uint64(firstByte)), start + 1, nil
	}
	numBytes := bits.LeadingZeros32(uint32(^firstByte)) - 24
	ret := uint64(firstByte & (0xff >> uint(numBytes)))
	if len(data) < start+numBytes+1 {
		return 0, 0, fmt.Errorf("data expect to have %d bytes, but it has only %d", start+numBytes+1, len(data))
	}
	for i := start; i < start+numBytes; i++ {
		ret <<= 8
		ret |= uint64(data[i+1] & 0xff)
	}
	return decIntZigZag(ret), start + numBytes + 1, nil
}

func decIntZigZag(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

func encIntZigZag(n int64) uint64 {
	return uint64((n >> 63) ^ (n << 1))
}

func encVints(months int32, seconds int32, nanos int64) []byte {
	buf := append(encVint(int64(months)), encVint(int64(seconds))...)
	return append(buf, encVint(nanos)...)
}

func encVint(v int64) []byte {
	vEnc := encIntZigZag(v)
	lead0 := bits.LeadingZeros64(vEnc)
	numBytes := (639 - lead0*9) >> 6

	// It can be 1 or 0 is v ==0
	if numBytes <= 1 {
		return []byte{byte(vEnc)}
	}
	extraBytes := numBytes - 1
	var buf = make([]byte, numBytes)
	for i := extraBytes; i >= 0; i-- {
		buf[i] = byte(vEnc)
		vEnc >>= 8
	}
	buf[0] |= byte(^(0xff >> uint(extraBytes)))
	return buf
}

type listSetCQLType struct {
	typ Type
}

var listSetCQLTypeParams = []reflect.Type{
	typeInfoType, // Elem
}

// Params returns the types to build the slice of params for TypeInfoFromParams.
func (listSetCQLType) Params(proto int) []reflect.Type {
	return listSetCQLTypeParams
}

// TypeInfoFromParams builds a TypeInfo implementation for the composite type with
// the given parameters.
func (t listSetCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 1 {
		panic(fmt.Errorf("expected 1 param for list/set, got %d", len(params)))
	}
	elem, ok := params[0].(TypeInfo)
	if !ok {
		panic(fmt.Errorf("expected TypeInfo for list/set, got %T", params[0]))
	}
	return CollectionType{
		NativeType: NativeType{proto: byte(proto), typ: t.typ},
		Elem:       elem,
	}
}

// TypeInfoFromString builds a TypeInfo implementation for the composite type with
// the given names/classes. Only the portion within the parantheses or arrows
// are passed to this function.
func (t listSetCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	elem := getCassandraTypeInfo(proto, name)
	return CollectionType{
		NativeType: NativeType{proto: byte(proto), typ: t.typ},
		Elem:       elem,
	}
}

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

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (listSetCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	listInfo, ok := info.(CollectionType)
	if !ok {
		return nil, marshalErrorf("marshal: can not marshal non collection type into list")
	}

	if value == nil {
		return nil, nil
	} else if _, ok := value.(unsetColumn); ok {
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
			return (listSetCQLType{}).Marshal(listInfo, keys)
		}
	}
	return nil, marshalErrorf("can not marshal %T into %s. Accepted types: slice, array, map[]struct.", value, info)
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

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (listSetCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	listInfo, ok := info.(CollectionType)
	if !ok {
		return unmarshalErrorf("unmarshal: can not unmarshal %T collection type into list", info)
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	if t.Kind() == reflect.Interface {
		if t.NumMethod() != 0 {
			return unmarshalErrorf("can not unmarshal into non-empty interface %T", value)
		}

		var elem interface{}
		// this relies on Unmarshal marshalling default values when presented with nil
		if err := Unmarshal(listInfo.Elem, []byte(nil), &elem); err != nil {
			return err
		}
		if elem == nil {
			panic(fmt.Errorf("elem was nil after unmarshalling from %s", listInfo.Elem))
		}

		t = reflect.SliceOf(reflect.TypeOf(elem))
	}

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
			if rv.Kind() == reflect.Interface {
				rv = rv.Elem()
			}
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
	return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: *slice, *array.", info, value)
}

type mapCQLType struct{}

var mapCQLTypeParams = []reflect.Type{
	typeInfoType, // Key
	typeInfoType, // Elem
}

// Params returns the types to build the slice of params for TypeInfoFromParams.
func (mapCQLType) Params(proto int) []reflect.Type {
	return mapCQLTypeParams
}

// TypeInfoFromParams builds a TypeInfo implementation for the composite type with
// the given parameters.
func (mapCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 2 {
		panic(fmt.Errorf("expected 2 param for map, got %d", len(params)))
	}
	key, ok := params[0].(TypeInfo)
	if !ok {
		panic(fmt.Errorf("expected TypeInfo for map, got %T", params[0]))
	}
	elem, ok := params[1].(TypeInfo)
	if !ok {
		panic(fmt.Errorf("expected TypeInfo for map, got %T", params[1]))
	}
	return CollectionType{
		NativeType: NativeType{proto: byte(proto), typ: TypeMap},
		Key:        key,
		Elem:       elem,
	}
}

// TypeInfoFromString builds a TypeInfo implementation for the composite type with
// the given names/classes. Only the portion within the parantheses or arrows
// are passed to this function.
func (mapCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	names := splitCompositeTypes(name)
	if len(names) != 2 {
		panic(fmt.Errorf("expected 2 elements for map, got %v", names))
	}
	return CollectionType{
		NativeType: NativeType{proto: byte(proto), typ: TypeMap},
		Key:        getCassandraTypeInfo(proto, names[0]),
		Elem:       getCassandraTypeInfo(proto, names[1]),
	}
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (mapCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	mapInfo, ok := info.(CollectionType)
	if !ok {
		return nil, marshalErrorf("marshal: can not marshal %T collection type into map", info)
	}

	if value == nil {
		return nil, nil
	} else if _, ok := value.(unsetColumn); ok {
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

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (mapCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	mapInfo, ok := info.(CollectionType)
	if !ok {
		return unmarshalErrorf("unmarshal: can not unmarshal %T collection type into map", info)
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal map into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	if t.Kind() == reflect.Interface {
		if t.NumMethod() != 0 {
			return unmarshalErrorf("can not unmarshal map into non-empty interface %T", value)
		}
		var key interface{}
		// this relies on Unmarshal marshalling default values when presented with nil
		if err := Unmarshal(mapInfo.Key, []byte(nil), &key); err != nil {
			return err
		}
		if key == nil {
			panic(fmt.Errorf("key was nil after unmarshalling from %s", mapInfo.Key))
		}

		var elem interface{}
		// this relies on Unmarshal marshalling default values when presented with nil
		if err := Unmarshal(mapInfo.Elem, []byte(nil), &elem); err != nil {
			return err
		}
		if elem == nil {
			panic(fmt.Errorf("elem was nil after unmarshalling from %s", mapInfo.Elem))
		}

		t = reflect.MapOf(reflect.TypeOf(key), reflect.TypeOf(elem))
	} else if t.Kind() != reflect.Map {
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
	if rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
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

type uuidCQLType struct{}

// uuidCQLType doesn't require any params
func (uuidCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (uuidCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for uuid type, got %d", len(params)))
	}
	return TypeUUID
}

// TypeInfoFromString returns the type itself.
func (uuidCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for uuid type, got %s", name))
	}
	return TypeUUID
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (uuidCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch val := value.(type) {
	case unsetColumn:
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

	return nil, marshalErrorf("can not marshal %T into %s. Accepted types: UUID, [16]byte, string, UnsetValue.", value, info)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (uuidCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	if len(data) == 0 {
		switch v := value.(type) {
		case *string:
			*v = ""
		case *[]byte:
			*v = nil
		case *UUID:
			*v = UUID{}
		case *interface{}:
			*v = UUID{}
		default:
			return unmarshalErrorf("can not unmarshal X %s into %T. Accepted types: *UUID, *[]byte, *string, *interface{}.", info, value)
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
	case *interface{}:
		var u UUID
		copy(u[:], data)
		*v = u
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
	return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: *UUID, *[]byte, *string, *interface{}.", info, value)
}

type timeUUIDCQLType struct{}

// timeUUIDCQLType doesn't require any params
func (timeUUIDCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (timeUUIDCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for timeuuid type, got %d", len(params)))
	}
	return TypeTimeUUID
}

// TypeInfoFromString returns the type itself.
func (timeUUIDCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for timeuuid type, got %s", name))
	}
	return TypeTimeUUID
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (timeUUIDCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	switch val := value.(type) {
	case time.Time:
		return UUIDFromTime(val).Bytes(), nil
	}
	return (uuidCQLType{}).Marshal(info, value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (timeUUIDCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
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
		return (uuidCQLType{}).Unmarshal(info, data, value)
	}
}

type inetCQLType struct{}

// inetCQLType doesn't require any params
func (inetCQLType) Params(proto int) []reflect.Type {
	return nil
}

// TypeInfoFromParams returns the type itself.
func (inetCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 0 {
		panic(fmt.Errorf("expected 0 param for inet type, got %d", len(params)))
	}
	return TypeInet
}

// TypeInfoFromString returns the type itself.
func (inetCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	if name != "" {
		panic(fmt.Errorf("expected empty name for inet type, got %s", name))
	}
	return TypeInet
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (inetCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	// we return either the 4 or 16 byte representation of an
	// ip address here otherwise the db value will be prefixed
	// with the remaining byte values e.g. ::ffff:127.0.0.1 and not 127.0.0.1
	switch val := value.(type) {
	case unsetColumn:
		return nil, nil
	case net.IP:
		t := val.To4()
		if t == nil {
			return val.To16(), nil
		}
		return t, nil
	case string:
		b := net.ParseIP(val)
		if b != nil {
			t := b.To4()
			if t == nil {
				return b.To16(), nil
			}
			return t, nil
		}
		return nil, marshalErrorf("cannot marshal. invalid ip string %s", val)
	}

	if value == nil {
		return nil, nil
	}

	return nil, marshalErrorf("cannot marshal %T into inet. Accepted types: net.IP, string.", value)
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (inetCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *net.IP:
		if len(data) == 0 {
			*v = nil
			return nil
		}
		if x := len(data); !(x == 4 || x == 16) {
			return unmarshalErrorf("cannot unmarshal inet into %T: invalid sized IP: got %d bytes not 4 or 16", value, x)
		}
		buf := copyBytes(data)
		ip := net.IP(buf)
		if v4 := ip.To4(); v4 != nil {
			*v = v4
			return nil
		}
		*v = ip
		return nil
	case *interface{}:
		if len(data) == 0 {
			*v = net.IP(nil)
			return nil
		}
		if x := len(data); !(x == 4 || x == 16) {
			return unmarshalErrorf("cannot unmarshal inet into %T: invalid sized IP: got %d bytes not 4 or 16", value, x)
		}
		buf := copyBytes(data)
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
	return unmarshalErrorf("cannot unmarshal inet into %T. Accepted types: Unmarshaler, *net.IP, *string, *interface{}.", value)
}

type tupleCQLType struct{}

var tupleCQLTypeParams = []reflect.Type{
	typeInfoSliceType, // Elems
}

// Params returns the types to build the slice of params for TypeInfoFromParams.
func (tupleCQLType) Params(proto int) []reflect.Type {
	return tupleCQLTypeParams
}

// TypeInfoFromParams builds a TypeInfo implementation for the composite type with
// the given parameters.
func (tupleCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 1 {
		panic(fmt.Errorf("expected 1 param for tuple, got %d", len(params)))
	}
	elems, ok := params[0].([]TypeInfo)
	if !ok {
		panic(fmt.Errorf("expected []TypeInfo for tuple, got %T", params[0]))
	}
	return TupleTypeInfo{
		Elems: elems,
	}
}

// TypeInfoFromString builds a TypeInfo implementation for the composite type with
// the given names/classes. Only the portion within the parantheses or arrows
// are passed to this function.
func (tupleCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	names := splitCompositeTypes(name)
	types := make([]TypeInfo, len(names))
	for i, name := range names {
		types[i] = getCassandraTypeInfo(proto, name)
	}
	return TupleTypeInfo{
		Elems: types,
	}
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (tupleCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	tuple := info.(TupleTypeInfo)
	switch v := value.(type) {
	case unsetColumn:
		return nil, unmarshalErrorf("Invalid request: UnsetValue is unsupported for tuples")
	case []interface{}:
		if len(v) != len(tuple.Elems) {
			return nil, unmarshalErrorf("cannont marshal tuple: wrong number of elements")
		}

		var buf []byte
		for i, elem := range v {
			if elem == nil {
				buf = appendInt(buf, int32(-1))
				continue
			}

			data, err := Marshal(tuple.Elems[i], elem)
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = appendInt(buf, int32(n))
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
				buf = appendInt(buf, int32(-1))
				continue
			}

			data, err := Marshal(elem, field.Interface())
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = appendInt(buf, int32(n))
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
				buf = appendInt(buf, int32(-1))
				continue
			}

			data, err := Marshal(elem, item.Interface())
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = appendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	}

	return nil, marshalErrorf("cannot marshal %T into tuple. Accepted types: struct, []interface{}, array, slice, UnsetValue.", value)
}

func readBytes(p []byte) ([]byte, []byte) {
	// TODO: really should use a framer
	size := readInt(p)
	p = p[4:]
	if size < 0 {
		return nil, p
	}
	return p[:size], p[size:]
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
// currently only support unmarshal into a list of values, this makes it possible
// to support tuples without changing the query API. In the future this can be extend
// to allow unmarshalling into custom tuple types.
func (tupleCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
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
				p, data = readBytes(data)
			}
			err := Unmarshal(elem, p, v[i])
			if err != nil {
				return err
			}
		}
		return nil
	case *interface{}:
		s := make([]interface{}, len(tuple.Elems))
		for i, elem := range tuple.Elems {
			// each element inside data is a [bytes]
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}
			err := Unmarshal(elem, p, &s[i])
			if err != nil {
				return err
			}
		}
		*v = s
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
				p, data = readBytes(data)
			}

			var v interface{}
			if err := Unmarshal(elem, p, &v); err != nil {
				return err
			}

			switch rv.Field(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					newv := reflect.New(reflect.TypeOf(v))
					newv.Elem().Set(reflect.ValueOf(v))
					rv.Field(i).Set(newv)
				} else {
					rv.Field(i).Set(reflect.Zero(rv.Field(i).Type()))
				}
			default:
				rv.Field(i).Set(reflect.ValueOf(v))
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
				p, data = readBytes(data)
			}

			var v interface{}
			if err := Unmarshal(elem, p, &v); err != nil {
				return err
			}

			switch rv.Index(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					newv := reflect.New(reflect.TypeOf(v))
					newv.Elem().Set(reflect.ValueOf(v))
					rv.Index(i).Set(newv)
				} else {
					rv.Index(i).Set(reflect.Zero(rv.Index(i).Type()))
				}
			default:
				rv.Index(i).Set(reflect.ValueOf(v))
			}
		}

		return nil
	}

	return unmarshalErrorf("cannot unmarshal tuple into %T. Accepted types: *struct, []interface{}, *array, *slice, *interface{}, Unmarshaler.", value)
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

type udtCQLType struct{}

var udtCQLTypeParams = []reflect.Type{
	stringType,        // Keyspace
	stringType,        // Name
	udtFieldSliceType, // Elements
}

// Params returns the types to build the slice of params for TypeInfoFromParams.
func (udtCQLType) Params(proto int) []reflect.Type {
	return udtCQLTypeParams
}

// TypeInfoFromParams builds a TypeInfo implementation for the composite type with
// the given parameters.
func (udtCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 3 {
		panic(fmt.Errorf("expected 3 param for udt, got %d", len(params)))
	}
	keyspace, ok := params[0].(string)
	if !ok {
		panic(fmt.Errorf("expected string for udt, got %T", params[0]))
	}
	name, ok := params[1].(string)
	if !ok {
		panic(fmt.Errorf("expected string for udt, got %T", params[1]))
	}
	elements, ok := params[2].([]UDTField)
	if !ok {
		panic(fmt.Errorf("expected []UDTField for udt, got %T", params[2]))
	}
	return UDTTypeInfo{
		KeySpace: keyspace,
		Name:     name,
		Elements: elements,
	}
}

// TypeInfoFromString builds a TypeInfo implementation for the composite type with
// the given names/classes. Only the portion within the parantheses or arrows
// are passed to this function.
func (udtCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	return NativeType{
		proto:  byte(proto),
		typ:    TypeCustom,
		custom: name,
	}
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (udtCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	udt := info.(UDTTypeInfo)

	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, unmarshalErrorf("invalid request: UnsetValue is unsupported for user defined types")
	case UDTMarshaler:
		var buf []byte
		for _, e := range udt.Elements {
			data, err := v.MarshalUDT(e.Name, e.Type)
			if err != nil {
				return nil, err
			}

			buf = appendBytes(buf, data)
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

			buf = appendBytes(buf, data)
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
		return nil, marshalErrorf("cannot marshal %T into %s. Accepted types: Marshaler, UDTMarshaler, map[string]interface{}, struct, UnsetValue.", value, info)
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

		buf = appendBytes(buf, data)
	}

	return buf, nil
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (udtCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	// do this up here so we don't need to duplicate all of the map logic below
	if iptr, ok := value.(*interface{}); ok && iptr != nil {
		v := map[string]interface{}{}
		*iptr = v
		value = &v
	}
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
			p, data = readBytes(data)
			if err := v.UnmarshalUDT(e.Name, e.Type, p); err != nil {
				return err
			}
		}

		return nil
	case *map[string]interface{}:
		udt := info.(UDTTypeInfo)

		if data == nil {
			*v = nil
			return nil
		}

		m := map[string]interface{}{}
		*v = m

		for id, e := range udt.Elements {
			if len(data) == 0 {
				return nil
			}
			if len(data) < 4 {
				return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", info, id, e.Name)
			}

			var p []byte
			p, data = readBytes(data)

			var v interface{}
			if err := Unmarshal(e.Type, p, &v); err != nil {
				return err
			}
			m[e.Name] = v
		}

		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	k := rv.Elem()
	if k.Kind() != reflect.Struct || !k.IsValid() {
		return unmarshalErrorf("cannot unmarshal %s into %T. Accepted types: Unmarshaler, UDTUnmarshaler, *map[string]interface{}, *struct.", info, value)
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
		p, data = readBytes(data)

		f, ok := fields[e.Name]
		if !ok {
			f = k.FieldByName(e.Name)
			if f == emptyValue {
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

// TypeInfo describes a Cassandra specific data type. Typically this is just a
// Type but can be a struct when more information is needed.
// TODO: move to types.go
type TypeInfo interface {
	Type() Type
}

// NativeType is a simple type that is defined by the Cassandra protocol.
// Deprecated. Use Type instead.
// TODO: move to types.go
type NativeType struct {
	proto  byte
	typ    Type
	custom string // only used for TypeCustom
}

// NewNativeType returns a NativeType with the given protocol version, type, and
// custom name.
// Deprecated. Use Type instead.
func NewNativeType(proto byte, typ Type, custom string) NativeType {
	return NativeType{proto, typ, custom}
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

// TODO: move to types.go
type CollectionType struct {
	NativeType
	Key  TypeInfo // only used for TypeMap
	Elem TypeInfo // only used for TypeMap, TypeList and TypeSet
}

func (c CollectionType) String() string {
	switch c.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", c.typ, c.Key, c.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", c.typ, c.Elem)
	default:
		return c.typ.String()
	}
}

// TODO: move to types.go
type TupleTypeInfo struct {
	Elems []TypeInfo
}

func (TupleTypeInfo) Type() Type {
	return TypeTuple
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", TypeTuple))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

type UDTField struct {
	Name string
	Type TypeInfo
}

type UDTTypeInfo struct {
	KeySpace string
	Name     string
	Elements []UDTField
}

func (u UDTTypeInfo) Type() Type {
	return TypeUDT
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

// Type is the identifier of a Cassandra internal datatype.
// TODO: move to types.go
type Type int

var _ TypeInfo = Type(0)

// TODO: move to types.go
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
	if r, ok := registeredTypes[t]; ok {
		return r.str
	}
	return fmt.Sprintf("unknown_type_%d", t)
}

// Type returns the identifier of the type. Necessary to implement the
// TypeInfo interface.
func (t Type) Type() Type {
	return t
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
