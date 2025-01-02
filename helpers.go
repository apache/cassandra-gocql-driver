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
	"fmt"
	"net"
	"reflect"
	"strings"
)

// RowData contains the column names and pointers to the default values for each
// column
type RowData struct {
	Columns []string
	Values  []interface{}
}

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}

// TODO: move to types.go
func getCassandraTypeInfo(proto int, name string) TypeInfo {
	if strings.HasPrefix(name, apacheCassandraTypePrefix) {
		name = apacheToCassandraType(name)
	}
	compositeNameIdx := strings.Index(name, "<")
	var typ Type
	if compositeNameIdx != -1 {
		// frozen is a special case
		if name[:compositeNameIdx] == "frozen" {
			return getCassandraTypeInfo(proto, name[compositeNameIdx+1:len(name)-1])
		}
		typ = getCassandraType(name[:compositeNameIdx])
		name = name[compositeNameIdx+1 : len(name)-1]
	} else {
		typ = getCassandraType(name)
		// clear the name since we just used it to get the type unless it was custom
		if typ != TypeCustom {
			name = ""
		}
	}
	rt := fastRegisteredTypeLookup(typ)
	if rt == nil {
		panic(fmt.Errorf("no registered type for %v", name))
	}
	return rt.TypeInfoFromString(proto, name)
}

// TODO: move to types.go
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

// TODO: move to types.go
func apacheToCassandraType(t string) string {
	t = strings.Replace(t, apacheCassandraTypePrefix, "", -1)
	t = strings.Replace(t, "(", "<", -1)
	t = strings.Replace(t, ")", ">", -1)
	types := strings.FieldsFunc(t, func(r rune) bool {
		return r == '<' || r == '>' || r == ','
	})
	for _, typ := range types {
		t = strings.Replace(t, typ, getCassandraType(typ).String(), -1)
	}
	// This is done so it exactly matches what Cassandra returns
	return strings.Replace(t, ",", ", ", -1)
}

// TODO: move to types.go
func getCassandraType(classOrName string) Type {
	classOrName = strings.TrimPrefix(classOrName, apacheCassandraTypePrefix)
	typ, ok := registeredTypesByString[classOrName]
	if !ok {
		typ = TypeCustom
	}
	return typ
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
			var val interface{}
			if err := Unmarshal(column.TypeInfo, []byte(nil), &val); err != nil {
				iter.err = err
				return RowData{}, err
			}
			columns = append(columns, column.Name)
			values = append(values, &val)
		} else {
			for i, elem := range c.Elems {
				columns = append(columns, TupleColumnName(column.Name, i))
				var val interface{}
				if err := Unmarshal(elem, []byte(nil), &val); err != nil {
					iter.err = err
					return RowData{}, err
				}
				values = append(values, &val)
			}
		}
	}

	rowData := RowData{
		Columns: columns,
		Values:  values,
	}

	return rowData, nil
}

// SliceMap is a helper function to make the API easier to use
// returns the data from the query in the form of []map[string]interface{}
func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}

	numCols := len(iter.Columns())
	var dataToReturn []map[string]interface{}
	for {
		m := make(map[string]interface{}, numCols)
		// TODO: this isn't as efficient as it could be since it will build the
		// column names each time but it's simple
		if !iter.MapScan(m) {
			break
		}
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

	cols := iter.Columns()
	columnNames := make([]string, 0, len(cols))
	values := make([]interface{}, 0, len(cols))
	for _, column := range iter.Columns() {
		if c, ok := column.TypeInfo.(TupleTypeInfo); ok {
			for i := range c.Elems {
				columnName := TupleColumnName(column.Name, i)
				if dest, ok := m[columnName]; ok {
					// TODO: check if dest is a pointer and if not, error here
					values = append(values, dest)
				} else {
					values = append(values, new(interface{}))
				}
				columnNames = append(columnNames, columnName)
			}
		} else {
			if dest, ok := m[column.Name]; ok {
				// TODO: check if dest is a pointer and if not, error here
				values = append(values, dest)
			} else {
				values = append(values, new(interface{}))
			}
			columnNames = append(columnNames, column.Name)
		}
	}
	if iter.Scan(values...) {
		for i, name := range columnNames {
			if iptr, ok := values[i].(*interface{}); ok {
				m[name] = *iptr
			} else {
				// TODO: it seems wrong to dereference the values that were passed in
				// originally in the map but that's what it was doing before
				m[name] = dereference(values[i])
			}
		}
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
