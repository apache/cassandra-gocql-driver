package internal

import (
	"github.com/gocql/gocql/protocol"
	"strings"
)

func CopyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

func getApacheCassandraType(class string) protocol.Type {
	switch strings.TrimPrefix(class, apacheCassandraTypePrefix) {
	case "AsciiType":
		return protocol.TypeAscii
	case "LongType":
		return protocol.TypeBigInt
	case "BytesType":
		return protocol.TypeBlob
	case "BooleanType":
		return protocol.TypeBoolean
	case "CounterColumnType":
		return protocol.TypeCounter
	case "DecimalType":
		return protocol.TypeDecimal
	case "DoubleType":
		return protocol.TypeDouble
	case "FloatType":
		return protocol.TypeFloat
	case "Int32Type":
		return protocol.TypeInt
	case "ShortType":
		return protocol.TypeSmallInt
	case "ByteType":
		return protocol.TypeTinyInt
	case "TimeType":
		return protocol.TypeTime
	case "DateType", "TimestampType":
		return protocol.TypeTimestamp
	case "UUIDType", "LexicalUUIDType":
		return protocol.TypeUUID
	case "UTF8Type":
		return protocol.TypeVarchar
	case "IntegerType":
		return protocol.TypeVarint
	case "TimeUUIDType":
		return protocol.TypeTimeUUID
	case "InetAddressType":
		return protocol.TypeInet
	case "MapType":
		return protocol.TypeMap
	case "ListType":
		return protocol.TypeList
	case "SetType":
		return protocol.TypeSet
	case "TupleType":
		return protocol.TypeTuple
	case "DurationType":
		return protocol.TypeDuration
	default:
		return protocol.TypeCustom
	}
}

func getCassandraBaseType(name string) protocol.Type {
	switch name {
	case "ascii":
		return protocol.TypeAscii
	case "bigint":
		return protocol.TypeBigInt
	case "blob":
		return protocol.TypeBlob
	case "boolean":
		return protocol.TypeBoolean
	case "counter":
		return protocol.TypeCounter
	case "date":
		return protocol.TypeDate
	case "decimal":
		return protocol.TypeDecimal
	case "double":
		return protocol.TypeDouble
	case "duration":
		return protocol.TypeDuration
	case "float":
		return protocol.TypeFloat
	case "int":
		return protocol.TypeInt
	case "smallint":
		return protocol.TypeSmallInt
	case "tinyint":
		return protocol.TypeTinyInt
	case "time":
		return protocol.TypeTime
	case "timestamp":
		return protocol.TypeTimestamp
	case "uuid":
		return protocol.TypeUUID
	case "varchar":
		return protocol.TypeVarchar
	case "text":
		return protocol.TypeText
	case "varint":
		return protocol.TypeVarint
	case "timeuuid":
		return protocol.TypeTimeUUID
	case "inet":
		return protocol.TypeInet
	case "MapType":
		return protocol.TypeMap
	case "ListType":
		return protocol.TypeList
	case "SetType":
		return protocol.TypeSet
	case "TupleType":
		return protocol.TypeTuple
	default:
		return protocol.TypeCustom
	}
}

//func getCassandraType(name string, logger StdLogger) TypeInfo {
//	if strings.HasPrefix(name, "frozen<") {
//		return getCassandraType(strings.TrimPrefix(name[:len(name)-1], "frozen<"), logger)
//	} else if strings.HasPrefix(name, "set<") {
//		return CollectionType{
//			NativeType: NativeType{typ: TypeSet},
//			Elem:       getCassandraType(strings.TrimPrefix(name[:len(name)-1], "set<"), logger),
//		}
//	} else if strings.HasPrefix(name, "list<") {
//		return CollectionType{
//			NativeType: NativeType{typ: TypeList},
//			Elem:       getCassandraType(strings.TrimPrefix(name[:len(name)-1], "list<"), logger),
//		}
//	} else if strings.HasPrefix(name, "map<") {
//		names := splitCompositeTypes(strings.TrimPrefix(name[:len(name)-1], "map<"))
//		if len(names) != 2 {
//			logger.Printf("Error parsing map type, it has %d subelements, expecting 2\n", len(names))
//			return NativeType{
//				typ: TypeCustom,
//			}
//		}
//		return CollectionType{
//			NativeType: NativeType{typ: TypeMap},
//			Key:        getCassandraType(names[0], logger),
//			Elem:       getCassandraType(names[1], logger),
//		}
//	} else if strings.HasPrefix(name, "tuple<") {
//		names := splitCompositeTypes(strings.TrimPrefix(name[:len(name)-1], "tuple<"))
//		types := make([]TypeInfo, len(names))
//
//		for i, name := range names {
//			types[i] = getCassandraType(name, logger)
//		}
//
//		return TupleTypeInfo{
//			NativeType: NativeType{typ: TypeTuple},
//			Elems:      types,
//		}
//	} else {
//		return NativeType{
//			typ: getCassandraBaseType(name),
//		}
//	}
//}
