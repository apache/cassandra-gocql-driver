package protocol

import (
	"fmt"
	"github.com/gocql/gocql/internal/logger"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"strings"
	"time"
)

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

func CopyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

func getCassandraBaseType(name string) Type {
	switch name {
	case "ascii":
		return TypeAscii
	case "bigint":
		return TypeBigInt
	case "blob":
		return TypeBlob
	case "boolean":
		return TypeBoolean
	case "counter":
		return TypeCounter
	case "date":
		return TypeDate
	case "decimal":
		return TypeDecimal
	case "double":
		return TypeDouble
	case "duration":
		return TypeDuration
	case "float":
		return TypeFloat
	case "int":
		return TypeInt
	case "smallint":
		return TypeSmallInt
	case "tinyint":
		return TypeTinyInt
	case "time":
		return TypeTime
	case "timestamp":
		return TypeTimestamp
	case "uuid":
		return TypeUUID
	case "varchar":
		return TypeVarchar
	case "text":
		return TypeText
	case "varint":
		return TypeVarint
	case "timeuuid":
		return TypeTimeUUID
	case "inet":
		return TypeInet
	case "MapType":
		return TypeMap
	case "ListType":
		return TypeList
	case "SetType":
		return TypeSet
	case "TupleType":
		return TypeTuple
	default:
		return TypeCustom
	}
}

func GetCassandraType(name string, logger logger.StdLogger) TypeInfo {
	if strings.HasPrefix(name, "frozen<") {
		return GetCassandraType(strings.TrimPrefix(name[:len(name)-1], "frozen<"), logger)
	} else if strings.HasPrefix(name, "set<") {
		return CollectionType{
			NativeType: NativeType{Typ: TypeSet},
			Elem:       GetCassandraType(strings.TrimPrefix(name[:len(name)-1], "set<"), logger),
		}
	} else if strings.HasPrefix(name, "list<") {
		return CollectionType{
			NativeType: NativeType{Typ: TypeList},
			Elem:       GetCassandraType(strings.TrimPrefix(name[:len(name)-1], "list<"), logger),
		}
	} else if strings.HasPrefix(name, "map<") {
		names := splitCompositeTypes(strings.TrimPrefix(name[:len(name)-1], "map<"))
		if len(names) != 2 {
			logger.Printf("Error parsing map type, it has %d subelements, expecting 2\n", len(names))
			return NativeType{
				Typ: TypeCustom,
			}
		}
		return CollectionType{
			NativeType: NativeType{Typ: TypeMap},
			Key:        GetCassandraType(names[0], logger),
			Elem:       GetCassandraType(names[1], logger),
		}
	} else if strings.HasPrefix(name, "tuple<") {
		names := splitCompositeTypes(strings.TrimPrefix(name[:len(name)-1], "tuple<"))
		types := make([]TypeInfo, len(names))

		for i, name := range names {
			types[i] = GetCassandraType(name, logger)
		}

		return TupleTypeInfo{
			NativeType: NativeType{Typ: TypeTuple},
			Elems:      types,
		}
	} else {
		return NativeType{
			Typ: getCassandraBaseType(name),
		}
	}
}

func splitCompositeTypes(name string) []string {
	if !strings.Contains(name, "<") {
		return strings.Split(name, ", ")
	}
	var parts []string
	lessCount := 0
	segment := ""
	for _, char := range name {
		if char == ',' && lessCount == 0 {
			if segment != "" {
				parts = append(parts, strings.TrimSpace(segment))
			}
			segment = ""
			continue
		}
		segment += string(char)
		if char == '<' {
			lessCount++
		} else if char == '>' {
			lessCount--
		}
	}
	if segment != "" {
		parts = append(parts, strings.TrimSpace(segment))
	}
	return parts
}

func ApacheToCassandraType(t string) string {
	t = strings.Replace(t, ApacheCassandraTypePrefix, "", -1)
	t = strings.Replace(t, "(", "<", -1)
	t = strings.Replace(t, ")", ">", -1)
	types := strings.FieldsFunc(t, func(r rune) bool {
		return r == '<' || r == '>' || r == ','
	})
	for _, typ := range types {
		t = strings.Replace(t, typ, GetApacheCassandraType(typ).String(), -1)
	}
	// This is done so it exactly matches what Cassandra returns
	return strings.Replace(t, ",", ", ", -1)
}

func GetApacheCassandraType(class string) Type {
	switch strings.TrimPrefix(class, ApacheCassandraTypePrefix) {
	case "AsciiType":
		return TypeAscii
	case "LongType":
		return TypeBigInt
	case "BytesType":
		return TypeBlob
	case "BooleanType":
		return TypeBoolean
	case "CounterColumnType":
		return TypeCounter
	case "DecimalType":
		return TypeDecimal
	case "DoubleType":
		return TypeDouble
	case "FloatType":
		return TypeFloat
	case "Int32Type":
		return TypeInt
	case "ShortType":
		return TypeSmallInt
	case "ByteType":
		return TypeTinyInt
	case "TimeType":
		return TypeTime
	case "DateType", "TimestampType":
		return TypeTimestamp
	case "UUIDType", "LexicalUUIDType":
		return TypeUUID
	case "UTF8Type":
		return TypeVarchar
	case "IntegerType":
		return TypeVarint
	case "TimeUUIDType":
		return TypeTimeUUID
	case "InetAddressType":
		return TypeInet
	case "MapType":
		return TypeMap
	case "ListType":
		return TypeList
	case "SetType":
		return TypeSet
	case "TupleType":
		return TypeTuple
	case "DurationType":
		return TypeDuration
	default:
		return TypeCustom
	}
}
