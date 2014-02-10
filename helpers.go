package gocql

import "reflect"

func (t *TypeInfo) New() interface{} {
	return reflect.New(goType(t)).Interface()
}

func goType(t *TypeInfo) reflect.Type {
	switch t.Type {
	case TypeVarchar, TypeAscii:
		return reflect.TypeOf(*new(string))
	case TypeBigInt, TypeCounter, TypeTimestamp:
		return reflect.TypeOf(*new(int64))
	case TypeBlob:
		return reflect.TypeOf(*new([]byte))
	case TypeBoolean:
		return reflect.TypeOf(*new(bool))
	case TypeFloat:
		return reflect.TypeOf(*new(float32))
	case TypeDouble:
		return reflect.TypeOf(*new(float64))
	case TypeInt:
		return reflect.TypeOf(*new(int))
	case TypeUUID, TypeTimeUUID:
		return reflect.TypeOf(*new(UUID))
	case TypeList, TypeSet:
		return reflect.SliceOf(goType(t.Elem))
	case TypeMap:
		return reflect.MapOf(goType(t.Key), goType(t.Elem))
	default:
		return nil
	}
}

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}

func (iter *Iter) RowData() (map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}
	rowData := make(map[string]interface{})
	for _, column := range iter.Columns() {
		rowData[column.Name] = column.TypeInfo.New()
	}
	return rowData, nil
}

// SliceMap is a helper function to make the API easier to use
// returns the data from the query in the form of []map[string]interface{}
func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}
	interfacesToScan := make([]interface{}, 0)
	for _, column := range iter.Columns() {
		i := column.TypeInfo.New()
		interfacesToScan = append(interfacesToScan, i)
	}
	dataToReturn := make([]map[string]interface{}, 0)
	for iter.Scan(interfacesToScan...) {
		m := make(map[string]interface{})
		for i, column := range iter.Columns() {
			m[column.Name] = dereference(interfacesToScan[i])
		}
		dataToReturn = append(dataToReturn, m)
	}
	if iter.err != nil {
		return nil, iter.err
	}
	return dataToReturn, nil
}

func (iter *Iter) MapScan(m map[string]interface{}) bool {
	if iter.err != nil {
		return false
	}
	interfacesToScan := make([]interface{}, 0)
	for _, column := range iter.Columns() {
		i := column.TypeInfo.New()
		interfacesToScan = append(interfacesToScan, i)
	}
	if iter.Scan(interfacesToScan...) {
		for i, column := range iter.Columns() {
			m[column.Name] = dereference(interfacesToScan[i])
		}
		return true
	}
	return false
}
