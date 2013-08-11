// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"time"
)

// Marshaler is the interface implemented by objects that can marshal
// themselves into values understood by Cassandra.
type Marshaler interface {
	MarshalCQL(info *TypeInfo, value interface{}) ([]byte, error)
}

// Unmarshaler is the interface implemented by objects that can unmarshal
// a Cassandra specific description of themselves.
type Unmarshaler interface {
	UnmarshalCQL(info *TypeInfo, data []byte, value interface{}) error
}

// Marshal returns the CQL encoding of the value for the Cassandra
// internal type described by the info parameter.
func Marshal(info *TypeInfo, value interface{}) ([]byte, error) {
	if v, ok := value.(Marshaler); ok {
		return v.MarshalCQL(info, value)
	}
	switch info.Type {
	case TypeVarchar, TypeAscii, TypeBlob:
		switch v := value.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		}
	case TypeBoolean:
		if v, ok := value.(bool); ok {
			if v {
				return []byte{1}, nil
			} else {
				return []byte{0}, nil
			}
		}
	case TypeInt:
		switch v := value.(type) {
		case int:
			x := int32(v)
			return []byte{byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}, nil
		}
	case TypeTimestamp:
		if v, ok := value.(time.Time); ok {
			x := v.In(time.UTC).UnixNano() / int64(time.Millisecond)
			return []byte{byte(x >> 56), byte(x >> 48), byte(x >> 40),
				byte(x >> 32), byte(x >> 24), byte(x >> 16),
				byte(x >> 8), byte(x)}, nil
		}
	}
	// TODO(tux21b): add reflection and a lot of other types
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

// Unmarshal parses the CQL encoded data based on the info parameter that
// describes the Cassandra internal data type and stores the result in the
// value pointed by value.
func Unmarshal(info *TypeInfo, data []byte, value interface{}) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data, value)
	}
	switch info.Type {
	case TypeVarchar, TypeAscii, TypeBlob:
		switch v := value.(type) {
		case *string:
			*v = string(data)
			return nil
		case *[]byte:
			val := make([]byte, len(data))
			copy(val, data)
			*v = val
			return nil
		}
	case TypeBoolean:
		if v, ok := value.(*bool); ok && len(data) == 1 {
			*v = data[0] != 0
			return nil
		}
	case TypeBigInt:
		if v, ok := value.(*int); ok && len(data) == 8 {
			*v = int(data[0])<<56 | int(data[1])<<48 | int(data[2])<<40 |
				int(data[3])<<32 | int(data[4])<<24 | int(data[5])<<16 |
				int(data[6])<<8 | int(data[7])
			return nil
		}
	case TypeInt:
		if v, ok := value.(*int); ok && len(data) == 4 {
			*v = int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 |
				int(data[3])
			return nil
		}
	case TypeTimestamp:
		if v, ok := value.(*time.Time); ok && len(data) == 8 {
			x := int64(data[0])<<56 | int64(data[1])<<48 |
				int64(data[2])<<40 | int64(data[3])<<32 |
				int64(data[4])<<24 | int64(data[5])<<16 |
				int64(data[6])<<8 | int64(data[7])
			sec := x / 1000
			nsec := (x - sec*1000) * 1000000
			*v = time.Unix(sec, nsec)
			return nil
		}
	}
	// TODO(tux21b): add reflection and a lot of other basic types
	return fmt.Errorf("can not unmarshal %s into %T", info, value)
}

// TypeInfo describes a Cassandra specific data type.
type TypeInfo struct {
	Type   Type
	Key    *TypeInfo // only used for TypeMap
	Value  *TypeInfo // only used for TypeMap, TypeList and TypeSet
	Custom string    // only used for TypeCostum
}

// String returns a human readable name for the Cassandra datatype
// described by t.
func (t TypeInfo) String() string {
	switch t.Type {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", t.Type, t.Key, t.Value)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", t.Type, t.Value)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", t.Type, t.Custom)
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
