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

package gocql

import (
	"fmt"
	"reflect"
)

// CQLType is the interface that must be implemented by all registered types.
type CQLType interface {
	// Params should return the types to build the slice of params for
	// TypeInfoFromParams. These params are sent to TypeInfoFromParams after being read
	// from the frame. The supported types are: Type, TypeInfo, []UDTField,
	// []byte, string, int, byte.
	// If no params are needed this can return a nil slice.
	Params(proto int) []reflect.Type

	// TypeInfoFromParams should return a TypeInfo implementation for the type
	// with the given parameters filled after the return from Params().
	TypeInfoFromParams(proto int, params []interface{}) TypeInfo

	// TypeInfoFromString should return a TypeInfo implementation for the type with
	// the given names/classes. Only the portion within the parantheses or arrows
	// are passed to this function. For simple types, the name passed might be empty.
	TypeInfoFromString(proto int, name string) TypeInfo

	// Marshal should marshal the value for the given TypeInfo into a byte slice
	Marshal(info TypeInfo, value interface{}) ([]byte, error)

	// Unmarshal should unmarshal the byte slice into the value for the given
	// TypeInfo. It must support data being nil for nullable columns and it must
	// support pointers to empty interfaces to get the default go type for the
	// column.
	Unmarshal(info TypeInfo, data []byte, value interface{}) error
}

var (
	asciiRegisteredType = &registeredType{
		CQLType: varcharLikeCQLType{
			typ: TypeAscii,
		},
		str: "ascii",
	}

	bigIntRegisteredType = &registeredType{
		CQLType: bigIntLikeCQLType{
			typ: TypeBigInt,
		},
		str: "bigint",
	}

	blobRegisteredType = &registeredType{
		CQLType: varcharLikeCQLType{
			typ: TypeBlob,
		},
		str: "blob",
	}

	booleanRegisteredType = &registeredType{
		CQLType: booleanCQLType{},
		str:     "boolean",
	}

	counterRegisteredType = &registeredType{
		CQLType: bigIntLikeCQLType{
			typ: TypeCounter,
		},
		str: "counter",
	}

	dateRegisteredType = &registeredType{
		CQLType: dateCQLType{},
		str:     "date",
	}

	decimalRegisteredType = &registeredType{
		CQLType: decimalCQLType{},
		str:     "decimal",
	}

	doubleRegisteredType = &registeredType{
		CQLType: doubleCQLType{},
		str:     "double",
	}

	durationRegisteredType = &registeredType{
		CQLType: durationCQLType{},
		str:     "duration",
	}

	floatRegisteredType = &registeredType{
		CQLType: floatCQLType{},
		str:     "float",
	}

	inetRegisteredType = &registeredType{
		CQLType: inetCQLType{},
		str:     "inet",
	}

	intRegisteredType = &registeredType{
		CQLType: intCQLType{},
		str:     "int",
	}

	smallintRegisteredType = &registeredType{
		CQLType: smallIntCQLType{},
		str:     "smallint",
	}

	textRegisteredType = &registeredType{
		CQLType: varcharLikeCQLType{
			typ: TypeText,
		},
		str: "text",
	}

	timeRegisteredType = &registeredType{
		CQLType: timeCQLType{},
		str:     "time",
	}

	timestampRegisteredType = &registeredType{
		CQLType: timestampCQLType{},
		str:     "timestamp",
	}

	timeUUIDRegisteredType = &registeredType{
		CQLType: timeUUIDCQLType{},
		str:     "timeuuid",
	}

	tinyIntRegisteredType = &registeredType{
		CQLType: tinyIntCQLType{},
		str:     "tinyint",
	}

	uuidRegisteredType = &registeredType{
		CQLType: uuidCQLType{},
		str:     "uuid",
	}

	varcharRegisteredType = &registeredType{
		CQLType: varcharLikeCQLType{
			typ: TypeVarchar,
		},
		str: "varchar",
	}

	varintRegisteredType = &registeredType{
		CQLType: varintCQLType{},
		str:     "varint",
	}

	listRegisteredType = &registeredType{
		CQLType: listSetCQLType{
			typ: TypeList,
		},
		str: "list",
	}

	mapRegisteredType = &registeredType{
		CQLType: mapCQLType{},
		str:     "map",
	}

	setRegisteredType = &registeredType{
		CQLType: listSetCQLType{
			typ: TypeSet,
		},
		str: "set",
	}

	tupleRegisteredType = &registeredType{
		CQLType: tupleCQLType{},
		str:     "tuple",
	}

	udtRegisteredType = &registeredType{
		CQLType: udtCQLType{},
		str:     "udt",
	}

	customRegisteredType = &registeredType{
		CQLType: customCQLType{},
		str:     "custom",
	}
)

var registeredTypes = map[Type]*registeredType{
	TypeAscii:     asciiRegisteredType,
	TypeBigInt:    bigIntRegisteredType,
	TypeBlob:      blobRegisteredType,
	TypeBoolean:   booleanRegisteredType,
	TypeCounter:   counterRegisteredType,
	TypeDate:      dateRegisteredType,
	TypeDecimal:   decimalRegisteredType,
	TypeDouble:    doubleRegisteredType,
	TypeDuration:  durationRegisteredType,
	TypeFloat:     floatRegisteredType,
	TypeInet:      inetRegisteredType,
	TypeInt:       intRegisteredType,
	TypeSmallInt:  smallintRegisteredType,
	TypeText:      textRegisteredType,
	TypeTime:      timeRegisteredType,
	TypeTimestamp: timestampRegisteredType,
	TypeTimeUUID:  timeUUIDRegisteredType,
	TypeTinyInt:   tinyIntRegisteredType,
	TypeUUID:      uuidRegisteredType,
	TypeVarchar:   varcharRegisteredType,
	TypeVarint:    varintRegisteredType,
	TypeList:      listRegisteredType,
	TypeMap:       mapRegisteredType,
	TypeSet:       setRegisteredType,
	TypeTuple:     tupleRegisteredType,
	TypeUDT:       udtRegisteredType,
	TypeCustom:    customRegisteredType,
}

// fastRegisteredTypeLookup is a fast lookup for the registered type that avoids
// the need for a map lookup which was shown to be significant
// in cases where it's necessary you should consider manually inlining this method
func fastRegisteredTypeLookup(typ Type) CQLType {
	switch typ {
	case TypeAscii:
		return asciiRegisteredType.CQLType
	case TypeBigInt:
		return bigIntRegisteredType.CQLType
	case TypeBlob:
		return blobRegisteredType.CQLType
	case TypeBoolean:
		return booleanRegisteredType.CQLType
	case TypeCounter:
		return counterRegisteredType.CQLType
	case TypeDate:
		return dateRegisteredType.CQLType
	case TypeDecimal:
		return decimalRegisteredType.CQLType
	case TypeDouble:
		return doubleRegisteredType.CQLType
	case TypeDuration:
		return durationRegisteredType.CQLType
	case TypeFloat:
		return floatRegisteredType.CQLType
	case TypeInet:
		return inetRegisteredType.CQLType
	case TypeInt:
		return intRegisteredType.CQLType
	case TypeSmallInt:
		return smallintRegisteredType.CQLType
	case TypeText:
		return textRegisteredType.CQLType
	case TypeTime:
		return timeRegisteredType.CQLType
	case TypeTimestamp:
		return timestampRegisteredType.CQLType
	case TypeTimeUUID:
		return timeUUIDRegisteredType.CQLType
	case TypeTinyInt:
		return tinyIntRegisteredType.CQLType
	case TypeUUID:
		return uuidRegisteredType.CQLType
	case TypeVarchar:
		return varcharRegisteredType.CQLType
	case TypeVarint:
		return varintRegisteredType.CQLType
	case TypeList:
		return listRegisteredType.CQLType
	case TypeMap:
		return mapRegisteredType.CQLType
	case TypeSet:
		return setRegisteredType.CQLType
	case TypeTuple:
		return tupleRegisteredType.CQLType
	case TypeUDT:
		return udtRegisteredType.CQLType
	case TypeCustom:
		return customRegisteredType.CQLType
	default:
		rt, ok := registeredTypes[typ]
		if !ok {
			return nil
		}
		return rt.CQLType
	}
}

var registeredTypesByString = map[string]Type{
	"ascii":     TypeAscii,
	"bigint":    TypeBigInt,
	"blob":      TypeBlob,
	"boolean":   TypeBoolean,
	"counter":   TypeCounter,
	"date":      TypeDate,
	"decimal":   TypeDecimal,
	"double":    TypeDouble,
	"duration":  TypeDuration,
	"float":     TypeFloat,
	"inet":      TypeInet,
	"int":       TypeInt,
	"smallint":  TypeSmallInt,
	"text":      TypeText,
	"time":      TypeTime,
	"timestamp": TypeTimestamp,
	"timeuuid":  TypeTimeUUID,
	"tinyint":   TypeTinyInt,
	"uuid":      TypeUUID,
	"varchar":   TypeVarchar,
	"varint":    TypeVarint,
	"map":       TypeMap,
	"list":      TypeList,
	"set":       TypeSet,
	"tuple":     TypeTuple,

	// these are all for Cassandra 2.x and older
	"AsciiType":         TypeAscii,
	"LongType":          TypeBigInt,
	"BytesType":         TypeBlob,
	"BooleanType":       TypeBoolean,
	"CounterColumnType": TypeCounter,
	"DecimalType":       TypeDecimal,
	"DoubleType":        TypeDouble,
	"FloatType":         TypeFloat,
	"Int32Type":         TypeInt,
	"ShortType":         TypeSmallInt,
	"ByteType":          TypeTinyInt,
	"TimeType":          TypeTime,
	"DateType":          TypeTimestamp, // DateType was a timestamp when date didn't exist
	"TimestampType":     TypeTimestamp,
	"UUIDType":          TypeUUID,
	"LexicalUUIDType":   TypeUUID,
	"UTF8Type":          TypeVarchar,
	"IntegerType":       TypeVarint,
	"TimeUUIDType":      TypeTimeUUID,
	"InetAddressType":   TypeInet,
	"MapType":           TypeMap,
	"ListType":          TypeList,
	"SetType":           TypeSet,
	"TupleType":         TypeTuple,
}

type registeredType struct {
	CQLType
	str string
}

// RegisterType registers a new CQL data type. Type should be the CQL id for
// the type. Name is the name of the type as returned in the metadata for the
// column. CQLType is the implementation of the type.
// This function is not goroutine-safe and must be called before any other usage
// of the gocql package. Typically this should be called in an init function.
func RegisterType(typ Type, name string, t CQLType) {
	if reg, ok := registeredTypes[typ]; ok {
		panic(fmt.Errorf("type %d already registered to %v", typ, reg.str))
	}
	registeredTypes[typ] = &registeredType{
		CQLType: t,
		str:     name,
	}
	if reg, ok := registeredTypesByString[name]; ok {
		panic(fmt.Errorf("type name %s already registered to %v", name, reg.String()))
	}
	registeredTypesByString[name] = typ
}

type customCQLType struct{}

var customCQLTypeParams = []reflect.Type{
	stringType, // name
}

// Params returns the types to build the slice of params for TypeInfoFromParams.
func (t customCQLType) Params(proto int) []reflect.Type {
	return customCQLTypeParams
}

// TypeInfoFromParams builds a TypeInfo implementation for the composite type with
// the given parameters.
func (t customCQLType) TypeInfoFromParams(proto int, params []interface{}) TypeInfo {
	if len(params) != 1 {
		panic(fmt.Errorf("expected 1 param for custom, got %d", len(params)))
	}
	custom, ok := params[0].(string)
	if !ok {
		panic(fmt.Errorf("expected string for custom, got %T", params[0]))
	}
	// TODO: ideally we'd return something else but this already exists
	return NativeType{
		proto:  byte(proto),
		typ:    getCassandraType(custom),
		custom: custom,
	}
}

// TypeInfoFromString builds a TypeInfo implementation for the composite type with
// the given names/classes. Only the portion within the parantheses or arrows
// are passed to this function.
func (t customCQLType) TypeInfoFromString(proto int, name string) TypeInfo {
	return NativeType{
		proto:  byte(proto),
		typ:    getCassandraType(name),
		custom: name,
	}
}

// Marshal marshals the value for the given TypeInfo into a byte slice.
func (t customCQLType) Marshal(info TypeInfo, value interface{}) ([]byte, error) {
	panic("not implemented")
}

// Unmarshal unmarshals the byte slice into the value for the given TypeInfo.
func (t customCQLType) Unmarshal(info TypeInfo, data []byte, value interface{}) error {
	panic("not implemented")
}
