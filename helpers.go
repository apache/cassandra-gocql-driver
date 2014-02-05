package gocql

func ptrOfType(t string) interface{} {
	switch t {
	case "ascii", "varchar", "inet":
		return new(string)
	case "bigint", "counter", "timestamp":
		return new(int64)
	case "blob":
		return new([]byte)
	case "boolean":
		return new(bool)
	case "decimal", "float":
		return new(float32)
	case "double":
		return new(float64)
	case "int":
		return new(int32)
	case "uuid", "timeuuid":
		return new(UUID)
	case "list", "set":
		return new([]interface{})
	case "map":
		return new(map[interface{}]interface{})
	default:
		return new([]byte)
	}
}

func dereference(i interface{}) interface{} {
	switch i.(type) {
	case *string:
		return *i.(*string)
	case *int64:
		return *i.(*int64)
	case *[]byte:
		return *i.(*[]byte)
	case *bool:
		return *i.(*bool)
	case *float32:
		return *i.(*float32)
	case *float64:
		return *i.(*float64)
	case *int32:
		return *i.(*int32)
	case *UUID:
		return *i.(*UUID)
	case *[]interface{}:
		return *i.(*[]interface{})
	case *map[interface{}]interface{}:
		return *i.(*map[interface{}]interface{})
	default:
		return i
	}
}

// SliceMap is a helper function to make the API easier to use
// returns the data from the query in the form of []map[string]interface{}
func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}
	interfacesToScan := make([]interface{}, 0)
	for _, column := range iter.Columns() {
		i := ptrOfType(column.TypeInfo.String())
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
	return dataToReturn, nil
}
