package gocql

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

var marshalTests = []struct {
	Info  *TypeInfo
	Data  []byte
	Value interface{}
}{
	{
		&TypeInfo{Type: TypeVarchar},
		[]byte("hello world"),
		[]byte("hello world"),
	},
	{
		&TypeInfo{Type: TypeVarchar},
		[]byte("hello world"),
		"hello world",
	},
	{
		&TypeInfo{Type: TypeVarchar},
		[]byte(nil),
		[]byte(nil),
	},
	{
		&TypeInfo{Type: TypeVarchar},
		[]byte("hello world"),
		MyString("hello world"),
	},
	{
		&TypeInfo{Type: TypeVarchar},
		[]byte("HELLO WORLD"),
		CustomString("hello world"),
	},
	{
		&TypeInfo{Type: TypeBlob},
		[]byte("hello\x00"),
		[]byte("hello\x00"),
	},
	{
		&TypeInfo{Type: TypeBlob},
		[]byte(nil),
		[]byte(nil),
	},
	{
		&TypeInfo{Type: TypeTimeUUID},
		[]byte{0x3d, 0xcd, 0x98, 0x0, 0xf3, 0xd9, 0x11, 0xbf, 0x86, 0xd4, 0xb8, 0xe8, 0x56, 0x2c, 0xc, 0xd0},
		func() UUID {
			x, _ := UUIDFromBytes([]byte{0x3d, 0xcd, 0x98, 0x0, 0xf3, 0xd9, 0x11, 0xbf, 0x86, 0xd4, 0xb8, 0xe8, 0x56, 0x2c, 0xc, 0xd0})
			return x
		}(),
	},
	{
		&TypeInfo{Type: TypeInt},
		[]byte("\x00\x00\x00\x00"),
		0,
	},
	{
		&TypeInfo{Type: TypeInt},
		[]byte("\x01\x02\x03\x04"),
		16909060,
	},
	{
		&TypeInfo{Type: TypeInt},
		[]byte("\x80\x00\x00\x00"),
		int32(math.MinInt32),
	},
	{
		&TypeInfo{Type: TypeInt},
		[]byte("\x7f\xff\xff\xff"),
		int32(math.MaxInt32),
	},
	{
		&TypeInfo{Type: TypeBigInt},
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
		0,
	},
	{
		&TypeInfo{Type: TypeBigInt},
		[]byte("\x01\x02\x03\x04\x05\x06\x07\x08"),
		72623859790382856,
	},
	{
		&TypeInfo{Type: TypeBigInt},
		[]byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
		int64(math.MinInt64),
	},
	{
		&TypeInfo{Type: TypeBigInt},
		[]byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
		int64(math.MaxInt64),
	},
	{
		&TypeInfo{Type: TypeBoolean},
		[]byte("\x00"),
		false,
	},
	{
		&TypeInfo{Type: TypeBoolean},
		[]byte("\x01"),
		true,
	},
	{
		&TypeInfo{Type: TypeFloat},
		[]byte("\x40\x49\x0f\xdb"),
		float32(3.14159265),
	},
	{
		&TypeInfo{Type: TypeDouble},
		[]byte("\x40\x09\x21\xfb\x53\xc8\xd4\xf1"),
		float64(3.14159265),
	},
	{
		&TypeInfo{Type: TypeTimestamp},
		[]byte("\x00\x00\x01\x40\x77\x16\xe1\xb8"),
		time.Date(2013, time.August, 13, 9, 52, 3, 0, time.UTC),
	},
	{
		&TypeInfo{Type: TypeTimestamp},
		[]byte("\x00\x00\x01\x40\x77\x16\xe1\xb8"),
		int64(1376387523000),
	},
	{
		&TypeInfo{Type: TypeList, Elem: &TypeInfo{Type: TypeInt}},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		[]int{1, 2},
	},
	{
		&TypeInfo{Type: TypeList, Elem: &TypeInfo{Type: TypeInt}},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		[2]int{1, 2},
	},
	{
		&TypeInfo{Type: TypeSet, Elem: &TypeInfo{Type: TypeInt}},
		[]byte("\x00\x02\x00\x04\x00\x00\x00\x01\x00\x04\x00\x00\x00\x02"),
		[]int{1, 2},
	},
	{
		&TypeInfo{Type: TypeSet, Elem: &TypeInfo{Type: TypeInt}},
		[]byte(nil),
		[]int(nil),
	},
	{
		&TypeInfo{Type: TypeMap,
			Key:  &TypeInfo{Type: TypeVarchar},
			Elem: &TypeInfo{Type: TypeInt},
		},
		[]byte("\x00\x01\x00\x03foo\x00\x04\x00\x00\x00\x01"),
		map[string]int{"foo": 1},
	},
	{
		&TypeInfo{Type: TypeMap,
			Key:  &TypeInfo{Type: TypeVarchar},
			Elem: &TypeInfo{Type: TypeInt},
		},
		[]byte(nil),
		map[string]int(nil),
	},
	{
		&TypeInfo{Type: TypeList, Elem: &TypeInfo{Type: TypeVarchar}},
		bytes.Join([][]byte{
			[]byte("\x00\x01\xFF\xFF"),
			bytes.Repeat([]byte("X"), 65535)}, []byte("")),
		[]string{strings.Repeat("X", 65535)},
	},
	{
		&TypeInfo{Type: TypeMap,
			Key:  &TypeInfo{Type: TypeVarchar},
			Elem: &TypeInfo{Type: TypeVarchar},
		},
		bytes.Join([][]byte{
			[]byte("\x00\x01\xFF\xFF"),
			bytes.Repeat([]byte("X"), 65535),
			[]byte("\xFF\xFF"),
			bytes.Repeat([]byte("Y"), 65535)}, []byte("")),
		map[string]string{
			strings.Repeat("X", 65535): strings.Repeat("Y", 65535),
		},
	},
}

func TestMarshal(t *testing.T) {
	for i, test := range marshalTests {
		data, err := Marshal(test.Info, test.Value)
		if err != nil {
			t.Errorf("marshalTest[%d]: %v", i, err)
			continue
		}
		if !bytes.Equal(data, test.Data) {
			t.Errorf("marshalTest[%d]: expected %q, got %q.", i, test.Data, data)
		}
	}
}

func TestUnmarshal(t *testing.T) {
	for i, test := range marshalTests {
		v := reflect.New(reflect.TypeOf(test.Value))
		err := Unmarshal(test.Info, test.Data, v.Interface())
		if err != nil {
			t.Errorf("marshalTest[%d]: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(v.Elem().Interface(), test.Value) {
			t.Errorf("marshalTest[%d]: expected %#v, got %#v.", i, test.Value, v.Elem().Interface())
		}
	}
}

type CustomString string

func (c CustomString) MarshalCQL(info *TypeInfo) ([]byte, error) {
	return []byte(strings.ToUpper(string(c))), nil
}
func (c *CustomString) UnmarshalCQL(info *TypeInfo, data []byte) error {
	*c = CustomString(strings.ToLower(string(data)))
	return nil
}

type MyString string

type MyInt int
