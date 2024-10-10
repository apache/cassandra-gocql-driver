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
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/inf.v0"
)

type RowData struct {
	Columns []string
	Values  []interface{}
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

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
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

// TODO: Cover with unit tests.
// Parses long Java-style type definition to internal data structures.
func getCassandraLongType(name string, protoVer byte, logger StdLogger) TypeInfo {
	if strings.HasPrefix(name, SET_TYPE) {
		return CollectionType{
			NativeType: NewNativeType(protoVer, TypeSet),
			Elem:       getCassandraLongType(unwrapCompositeTypeDefinition(name, SET_TYPE, '('), protoVer, logger),
		}
	} else if strings.HasPrefix(name, LIST_TYPE) {
		return CollectionType{
			NativeType: NewNativeType(protoVer, TypeList),
			Elem:       getCassandraLongType(unwrapCompositeTypeDefinition(name, LIST_TYPE, '('), protoVer, logger),
		}
	} else if strings.HasPrefix(name, MAP_TYPE) {
		names := splitJavaCompositeTypes(name, MAP_TYPE)
		if len(names) != 2 {
			logger.Printf("gocql: error parsing map type, it has %d subelements, expecting 2\n", len(names))
			return NewNativeType(protoVer, TypeCustom)
		}
		return CollectionType{
			NativeType: NewNativeType(protoVer, TypeMap),
			Key:        getCassandraLongType(names[0], protoVer, logger),
			Elem:       getCassandraLongType(names[1], protoVer, logger),
		}
	} else if strings.HasPrefix(name, TUPLE_TYPE) {
		names := splitJavaCompositeTypes(name, TUPLE_TYPE)
		types := make([]TypeInfo, len(names))

		for i, name := range names {
			types[i] = getCassandraLongType(name, protoVer, logger)
		}

		return TupleTypeInfo{
			NativeType: NewNativeType(protoVer, TypeTuple),
			Elems:      types,
		}
	} else if strings.HasPrefix(name, UDT_TYPE) {
		names := splitJavaCompositeTypes(name, UDT_TYPE)
		fields := make([]UDTField, len(names)-2)

		for i := 2; i < len(names); i++ {
			spec := strings.Split(names[i], ":")
			fieldName, _ := hex.DecodeString(spec[0])
			fields[i-2] = UDTField{
				Name: string(fieldName),
				Type: getCassandraLongType(spec[1], protoVer, logger),
			}
		}

		udtName, _ := hex.DecodeString(names[1])
		return UDTTypeInfo{
			NativeType: NewNativeType(protoVer, TypeUDT),
			KeySpace:   names[0],
			Name:       string(udtName),
			Elements:   fields,
		}
	} else if strings.HasPrefix(name, VECTOR_TYPE) {
		names := splitJavaCompositeTypes(name, VECTOR_TYPE)
		subType := getCassandraLongType(strings.TrimSpace(names[0]), protoVer, logger)
		dim, err := strconv.Atoi(strings.TrimSpace(names[1]))
		if err != nil {
			logger.Printf("gocql: error parsing vector dimensions: %v\n", err)
			return NewNativeType(protoVer, TypeCustom)
		}

		return VectorType{
			NativeType: NewCustomType(protoVer, TypeCustom, VECTOR_TYPE),
			SubType:    subType,
			Dimensions: dim,
		}
	} else {
		// basic type
		return NativeType{
			proto: protoVer,
			typ:   getApacheCassandraType(name),
		}
	}
}

// Parses short CQL type representation (e.g. map<text, text>) to internal data structures.
func getCassandraType(name string, protoVer byte, logger StdLogger) TypeInfo {
	if strings.HasPrefix(name, "frozen<") {
		return getCassandraType(unwrapCompositeTypeDefinition(name, "frozen", '<'), protoVer, logger)
	} else if strings.HasPrefix(name, "set<") {
		return CollectionType{
			NativeType: NewNativeType(protoVer, TypeSet),
			Elem:       getCassandraType(unwrapCompositeTypeDefinition(name, "set", '<'), protoVer, logger),
		}
	} else if strings.HasPrefix(name, "list<") {
		return CollectionType{
			NativeType: NewNativeType(protoVer, TypeList),
			Elem:       getCassandraType(unwrapCompositeTypeDefinition(name, "list", '<'), protoVer, logger),
		}
	} else if strings.HasPrefix(name, "map<") {
		names := splitCQLCompositeTypes(name, "map")
		if len(names) != 2 {
			logger.Printf("Error parsing map type, it has %d subelements, expecting 2\n", len(names))
			return NewNativeType(protoVer, TypeCustom)
		}
		return CollectionType{
			NativeType: NewNativeType(protoVer, TypeMap),
			Key:        getCassandraType(names[0], protoVer, logger),
			Elem:       getCassandraType(names[1], protoVer, logger),
		}
	} else if strings.HasPrefix(name, "tuple<") {
		names := splitCQLCompositeTypes(name, "tuple")
		types := make([]TypeInfo, len(names))

		for i, name := range names {
			types[i] = getCassandraType(name, protoVer, logger)
		}

		return TupleTypeInfo{
			NativeType: NewNativeType(protoVer, TypeTuple),
			Elems:      types,
		}
	} else if strings.HasPrefix(name, "vector<") {
		names := splitCQLCompositeTypes(name, "vector")
		subType := getCassandraType(strings.TrimSpace(names[0]), protoVer, logger)
		dim, _ := strconv.Atoi(strings.TrimSpace(names[1]))

		return VectorType{
			NativeType: NewCustomType(protoVer, TypeCustom, VECTOR_TYPE),
			SubType:    subType,
			Dimensions: dim,
		}
	} else {
		return NativeType{
			proto: protoVer,
			typ:   getCassandraBaseType(name),
		}
	}
}

func splitCQLCompositeTypes(name string, typeName string) []string {
	return splitCompositeTypes(name, typeName, '<', '>')
}

func splitJavaCompositeTypes(name string, typeName string) []string {
	return splitCompositeTypes(name, typeName, '(', ')')
}

func unwrapCompositeTypeDefinition(name string, typeName string, typeOpen int32) string {
	return strings.TrimPrefix(name[:len(name)-1], typeName+string(typeOpen))
}

func splitCompositeTypes(name string, typeName string, typeOpen int32, typeClose int32) []string {
	def := unwrapCompositeTypeDefinition(name, typeName, typeOpen)
	if !strings.Contains(def, string(typeOpen)) {
		parts := strings.Split(def, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	var parts []string
	lessCount := 0
	segment := ""
	for _, char := range def {
		if char == ',' && lessCount == 0 {
			if segment != "" {
				parts = append(parts, strings.TrimSpace(segment))
			}
			segment = ""
			continue
		}
		segment += string(char)
		if char == typeOpen {
			lessCount++
		} else if char == typeClose {
			lessCount--
		}
	}
	if segment != "" {
		parts = append(parts, strings.TrimSpace(segment))
	}
	return parts
}

func getApacheCassandraType(class string) Type {
	switch strings.TrimPrefix(class, apacheCassandraTypePrefix) {
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
	case "SimpleDateType":
		return TypeDate
	case "UserType":
		return TypeUDT
	default:
		return TypeCustom
	}
}

func (r *RowData) rowMap(m map[string]interface{}) {
	for i, column := range r.Columns {
		val := dereference(r.Values[i])
		if valVal := reflect.ValueOf(val); valVal.Kind() == reflect.Slice && !valVal.IsNil() {
			valCopy := reflect.MakeSlice(valVal.Type(), valVal.Len(), valVal.Cap())
			reflect.Copy(valCopy, valVal)
			m[column] = valCopy.Interface()
		} else {
			m[column] = val
		}
	}
}

// TupeColumnName will return the column name of a tuple value in a column named
// c at index n. It should be used if a specific element within a tuple is needed
// to be extracted from a map returned from SliceMap or MapScan.
func TupleColumnName(c string, n int) string {
	return fmt.Sprintf("%s[%d]", c, n)
}

// RowData returns the RowData for the iterator.
func (iter *Iter) RowData() (RowData, error) {
	if iter.err != nil {
		return RowData{}, iter.err
	}

	columns := make([]string, 0, len(iter.Columns()))
	values := make([]interface{}, 0, len(iter.Columns()))

	for _, column := range iter.Columns() {
		if c, ok := column.TypeInfo.(TupleTypeInfo); !ok {
			val, err := column.TypeInfo.NewWithError()
			if err != nil {
				iter.err = err
				return RowData{}, err
			}
			columns = append(columns, column.Name)
			values = append(values, val)
		} else {
			for i, elem := range c.Elems {
				columns = append(columns, TupleColumnName(column.Name, i))
				val, err := elem.NewWithError()
				if err != nil {
					iter.err = err
					return RowData{}, err
				}
				values = append(values, val)
			}
		}
	}

	rowData := RowData{
		Columns: columns,
		Values:  values,
	}

	return rowData, nil
}

// TODO(zariel): is it worth exporting this?
func (iter *Iter) rowMap() (map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}

	rowData, err := iter.RowData()
	if err != nil {
		return nil, err
	}
	iter.Scan(rowData.Values...)
	m := make(map[string]interface{}, len(rowData.Columns))
	rowData.rowMap(m)
	return m, nil
}

// SliceMap is a helper function to make the API easier to use
// returns the data from the query in the form of []map[string]interface{}
func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}

	// Not checking for the error because we just did
	rowData, err := iter.RowData()
	if err != nil {
		return nil, err
	}
	dataToReturn := make([]map[string]interface{}, 0)
	for iter.Scan(rowData.Values...) {
		m := make(map[string]interface{}, len(rowData.Columns))
		rowData.rowMap(m)
		dataToReturn = append(dataToReturn, m)
	}
	if iter.err != nil {
		return nil, iter.err
	}
	return dataToReturn, nil
}

// MapScan takes a map[string]interface{} and populates it with a row
// that is returned from cassandra.
//
// Each call to MapScan() must be called with a new map object.
// During the call to MapScan() any pointers in the existing map
// are replaced with non pointer types before the call returns
//
//	iter := session.Query(`SELECT * FROM mytable`).Iter()
//	for {
//		// New map each iteration
//		row := make(map[string]interface{})
//		if !iter.MapScan(row) {
//			break
//		}
//		// Do things with row
//		if fullname, ok := row["fullname"]; ok {
//			fmt.Printf("Full Name: %s\n", fullname)
//		}
//	}
//
// You can also pass pointers in the map before each call
//
//	var fullName FullName // Implements gocql.Unmarshaler and gocql.Marshaler interfaces
//	var address net.IP
//	var age int
//	iter := session.Query(`SELECT * FROM scan_map_table`).Iter()
//	for {
//		// New map each iteration
//		row := map[string]interface{}{
//			"fullname": &fullName,
//			"age":      &age,
//			"address":  &address,
//		}
//		if !iter.MapScan(row) {
//			break
//		}
//		fmt.Printf("First: %s Age: %d Address: %q\n", fullName.FirstName, age, address)
//	}
func (iter *Iter) MapScan(m map[string]interface{}) bool {
	if iter.err != nil {
		return false
	}

	rowData, err := iter.RowData()
	if err != nil {
		return false
	}

	for i, col := range rowData.Columns {
		if dest, ok := m[col]; ok {
			rowData.Values[i] = dest
		}
	}

	if iter.Scan(rowData.Values...) {
		rowData.rowMap(m)
		return true
	}
	return false
}

func copyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

var failDNS = false

func LookupIP(host string) ([]net.IP, error) {
	if failDNS {
		return nil, &net.DNSError{}
	}
	return net.LookupIP(host)

}
