package gocql

import "testing"

func BenchmarkMarshal(b *testing.B) {
	typeInfo := UDTTypeInfo{NativeType{proto: 3, typ: TypeUDT}, "", "xyz", []UDTField{
		{Name: "x", Type: &NativeType{proto: 3, typ: TypeInt}},
		{Name: "y", Type: &NativeType{proto: 3, typ: TypeInt}},
		{Name: "z", Type: &NativeType{proto: 3, typ: TypeInt}},
	}}
	type NewWithError struct {
		Value interface{}
		Error error
	}

	type ResultStore struct {
		NewWithError NewWithError
		New          interface{}
		String       string
		Type         Type
		Version      byte
		Custom       string
	}

	store := make([]ResultStore, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store[i].NewWithError.Value, store[i].NewWithError.Error = typeInfo.NewWithError()
		store[i].New = typeInfo.New()
		store[i].String = typeInfo.String()
		store[i].Type = typeInfo.Type()
		store[i].Version = typeInfo.Version()
		store[i].Custom = typeInfo.Custom()
	}
}
