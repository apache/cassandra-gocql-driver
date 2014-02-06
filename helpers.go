package gocql

func ptrOfType(t *TypeInfo) interface{} {
	switch t.Type {
	case TypeVarchar, TypeAscii:
		return new(string)
	case TypeBigInt, TypeCounter, TypeTimestamp:
		return new(int64)
	case TypeBlob:
		return new([]byte)
	case TypeBoolean:
		return new(bool)
	case TypeFloat:
		return new(float32)
	case TypeDouble:
		return new(float64)
	case TypeInt:
		return new(int32)
	case TypeUUID, TypeTimeUUID:
		return new(UUID)
	case TypeList, TypeSet:
		switch t.Elem.Type {
		case TypeVarchar, TypeAscii:
			return new([]string)
		case TypeBigInt, TypeCounter, TypeTimestamp:
			return new([]int64)
		case TypeBlob:
			return new([][]byte)
		case TypeBoolean:
			return new([]bool)
		case TypeFloat:
			return new([]float32)
		case TypeDouble:
			return new([]float64)
		case TypeInt:
			return new([]int32)
		case TypeUUID, TypeTimeUUID:
			return new([]UUID)
		}
		return new([][]byte)

	case TypeMap:
		switch t.Key.Type {
		case TypeVarchar, TypeAscii:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[string]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[string]int64)
			case TypeBlob:
				return new(map[string][]byte)
			case TypeBoolean:
				return new(map[string]bool)
			case TypeFloat:
				return new(map[string]float32)
			case TypeDouble:
				return new(map[string]float64)
			case TypeInt:
				return new(map[string]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[string]UUID)
			}			
		case TypeBigInt, TypeCounter, TypeTimestamp:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[int64]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[int64]int64)
			case TypeBlob:
				return new(map[int64][]byte)
			case TypeBoolean:
				return new(map[int64]bool)
			case TypeFloat:
				return new(map[int64]float32)
			case TypeDouble:
				return new(map[int64]float64)
			case TypeInt:
				return new(map[int64]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[int64]UUID)
			}
		case TypeBlob:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[string]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[string]int64)
			case TypeBlob:
				return new(map[string][]byte)
			case TypeBoolean:
				return new(map[string]bool)
			case TypeFloat:
				return new(map[string]float32)
			case TypeDouble:
				return new(map[string]float64)
			case TypeInt:
				return new(map[string]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[string]UUID)
			}
		case TypeBoolean:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[bool]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[bool]int64)
			case TypeBlob:
				return new(map[bool][]byte)
			case TypeBoolean:
				return new(map[bool]bool)
			case TypeFloat:
				return new(map[bool]float32)
			case TypeDouble:
				return new(map[bool]float64)
			case TypeInt:
				return new(map[bool]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[bool]UUID)
			}
		case TypeFloat:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[float32]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[float32]int64)
			case TypeBlob:
				return new(map[float32][]byte)
			case TypeBoolean:
				return new(map[float32]bool)
			case TypeFloat:
				return new(map[float32]float32)
			case TypeDouble:
				return new(map[float32]float64)
			case TypeInt:
				return new(map[float32]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[float32]UUID)
			}
		case TypeDouble:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[float64]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[float64]int64)
			case TypeBlob:
				return new(map[float64][]byte)
			case TypeBoolean:
				return new(map[float64]bool)
			case TypeFloat:
				return new(map[float64]float32)
			case TypeDouble:
				return new(map[float64]float64)
			case TypeInt:
				return new(map[float64]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[float64]UUID)
			}
		case TypeInt:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[int32]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[int32]int64)
			case TypeBlob:
				return new(map[int32][]byte)
			case TypeBoolean:
				return new(map[int32]bool)
			case TypeFloat:
				return new(map[int32]float32)
			case TypeDouble:
				return new(map[int32]float64)
			case TypeInt:
				return new(map[int32]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[int32]UUID)
			}
		case TypeUUID, TypeTimeUUID:
			switch t.Elem.Type {
			case TypeVarchar, TypeAscii:
				return new(map[UUID]string)
			case TypeBigInt, TypeCounter, TypeTimestamp:
				return new(map[UUID]int64)
			case TypeBlob:
				return new(map[UUID][]byte)
			case TypeBoolean:
				return new(map[UUID]bool)
			case TypeFloat:
				return new(map[UUID]float32)
			case TypeDouble:
				return new(map[UUID]float64)
			case TypeInt:
				return new(map[UUID]int32)
			case TypeUUID, TypeTimeUUID:
				return new(map[UUID]UUID)
			}
		}
	default:
		return new([]byte)
	}
	return new(map[string]string)
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
	case *[]string:
		return *i.(*[]string)
	case *[]int64:
		return *i.(*[]int64)
	case *[][]byte:
		return *i.(*[][]byte)
	case *[]bool:
		return *i.(*[]bool)
	case *[]float32:
		return *i.(*[]float32)
	case *[]float64:
		return *i.(*[]float64)
	case *[]int32:
		return *i.(*[]int32)
	case *[]UUID:
		return *i.(*[]UUID)
	case *map[string]string:
		return *i.(*map[string]string)
	case *map[string]int64:
		return *i.(*map[string]int64)
	case *map[string][]byte:
		return *i.(*map[string][]byte)
	case *map[string]bool:
		return *i.(*map[string]bool)
	case *map[string]float32:
		return *i.(*map[string]float32)
	case *map[string]float64:
		return *i.(*map[string]float64)
	case *map[string]int32:
		return *i.(*map[string]int32)
	case *map[string]UUID:
		return *i.(*map[string]UUID)
	case *map[int64]string:
		return *i.(*map[int64]string)
	case *map[int64]int64:
		return *i.(*map[int64]int64)
	case *map[int64][]byte:
		return *i.(*map[int64][]byte)
	case *map[int64]bool:
		return *i.(*map[int64]bool)
	case *map[int64]float32:
		return *i.(*map[int64]float32)
	case *map[int64]float64:
		return *i.(*map[int64]float64)
	case *map[int64]int32:
		return *i.(*map[int64]int32)
	case *map[int64]UUID:
		return *i.(*map[int64]UUID)
	case *map[bool]string:
		return *i.(*map[bool]string)
	case *map[bool]int64:
		return *i.(*map[bool]int64)
	case *map[bool][]byte:
		return *i.(*map[bool][]byte)
	case *map[bool]bool:
		return *i.(*map[bool]bool)
	case *map[bool]float32:
		return *i.(*map[bool]float32)
	case *map[bool]float64:
		return *i.(*map[bool]float64)
	case *map[bool]int32:
		return *i.(*map[bool]int32)
	case *map[bool]UUID:
		return *i.(*map[bool]UUID)
	case *map[float32]string:
		return *i.(*map[float32]string)
	case *map[float32]int64:
		return *i.(*map[float32]int64)
	case *map[float32][]byte:
		return *i.(*map[float32][]byte)
	case *map[float32]bool:
		return *i.(*map[float32]bool)
	case *map[float32]float32:
		return *i.(*map[float32]float32)
	case *map[float32]float64:
		return *i.(*map[float32]float64)
	case *map[float32]int32:
		return *i.(*map[float32]int32)
	case *map[float32]UUID:
		return *i.(*map[float32]UUID)
	case *map[float64]string:
		return *i.(*map[float64]string)
	case *map[float64]int64:
		return *i.(*map[float64]int64)
	case *map[float64][]byte:
		return *i.(*map[float64][]byte)
	case *map[float64]bool:
		return *i.(*map[float64]bool)
	case *map[float64]float32:
		return *i.(*map[float64]float32)
	case *map[float64]float64:
		return *i.(*map[float64]float64)
	case *map[float64]int32:
		return *i.(*map[float64]int32)
	case *map[float64]UUID:
		return *i.(*map[float64]UUID)
	case *map[int32]string:
		return *i.(*map[int32]string)
	case *map[int32]int64:
		return *i.(*map[int32]int64)
	case *map[int32][]byte:
		return *i.(*map[int32][]byte)
	case *map[int32]bool:
		return *i.(*map[int32]bool)
	case *map[int32]float32:
		return *i.(*map[int32]float32)
	case *map[int32]float64:
		return *i.(*map[int32]float64)
	case *map[int32]int32:
		return *i.(*map[int32]int32)
	case *map[int32]UUID:
		return *i.(*map[int32]UUID)
	case *map[UUID]string:
		return *i.(*map[UUID]string)
	case *map[UUID]int64:
		return *i.(*map[UUID]int64)
	case *map[UUID][]byte:
		return *i.(*map[UUID][]byte)
	case *map[UUID]bool:
		return *i.(*map[UUID]bool)
	case *map[UUID]float32:
		return *i.(*map[UUID]float32)
	case *map[UUID]float64:
		return *i.(*map[UUID]float64)
	case *map[UUID]int32:
		return *i.(*map[UUID]int32)
	case *map[UUID]UUID:
		return *i.(*map[UUID]UUID)
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
		i := ptrOfType(column.TypeInfo)
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
