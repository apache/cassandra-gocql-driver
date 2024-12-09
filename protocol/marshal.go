package protocol

import (
	"bytes"
	"fmt"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"time"
)

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
	Proto byte
	Typ   Type
	Cust  string // only used for TypeCustom
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
	return s.Typ
}

func (s NativeType) Version() byte {
	return s.Proto
}

func (s NativeType) Custom() string {
	return s.Cust
}

func (s NativeType) String() string {
	switch s.Typ {
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", s.Typ, s.Cust)
	default:
		return s.Typ.String()
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
	switch c.Typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", c.Typ, c.Key, c.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", c.Typ, c.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", c.Typ, c.Cust)
	default:
		return c.Typ.String()
	}
}

type TupleTypeInfo struct {
	NativeType
	Elems []TypeInfo
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.Typ))
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

func goType(t TypeInfo) (reflect.Type, error) {
	switch t.Type() {
	case TypeVarchar, TypeAscii, TypeInet, TypeText:
		return reflect.TypeOf(*new(string)), nil
	case TypeBigInt, TypeCounter:
		return reflect.TypeOf(*new(int64)), nil
	case TypeTime:
		return reflect.TypeOf(*new(time.Duration)), nil
	case TypeTimestamp:
		return reflect.TypeOf(*new(time.Time)), nil
	case TypeBlob:
		return reflect.TypeOf(*new([]byte)), nil
	case TypeBoolean:
		return reflect.TypeOf(*new(bool)), nil
	case TypeFloat:
		return reflect.TypeOf(*new(float32)), nil
	case TypeDouble:
		return reflect.TypeOf(*new(float64)), nil
	case TypeInt:
		return reflect.TypeOf(*new(int)), nil
	case TypeSmallInt:
		return reflect.TypeOf(*new(int16)), nil
	case TypeTinyInt:
		return reflect.TypeOf(*new(int8)), nil
	case TypeDecimal:
		return reflect.TypeOf(*new(*inf.Dec)), nil
	case TypeUUID, TypeTimeUUID:
		return reflect.TypeOf(*new(UUID)), nil
	case TypeList, TypeSet:
		elemType, err := goType(t.(CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil
	case TypeMap:
		keyType, err := goType(t.(CollectionType).Key)
		if err != nil {
			return nil, err
		}
		valueType, err := goType(t.(CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valueType), nil
	case TypeVarint:
		return reflect.TypeOf(*new(*big.Int)), nil
	case TypeTuple:
		// what can we do here? all there is to do is to make a list of interface{}
		tuple := t.(TupleTypeInfo)
		return reflect.TypeOf(make([]interface{}, len(tuple.Elems))), nil
	case TypeUDT:
		return reflect.TypeOf(make(map[string]interface{})), nil
	case TypeDate:
		return reflect.TypeOf(*new(time.Time)), nil
	case TypeDuration:
		return reflect.TypeOf(*new(Duration)), nil
	default:
		return nil, fmt.Errorf("cannot create Go type for unknown CQL type %s", t)
	}
}
