package gocql

import (
	"fmt"
	"github.com/gocql/gocql/pkg/writers/row"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkWriters_test(b *testing.B) {
	b.ReportAllocs()
	runWritersSet(10000, b)
	runWritersSet(100, b)
	runWritersSet(2, b)
}

func runWritersSet(rows int, b *testing.B) {
	setName := fmt.Sprintf("rows count %d", rows)
	data := randInts(rows, 3)
	b.Run(setName, func(b *testing.B) {
		fmt.Println("--------------------------------------", setName, "---------------------------------------")
		b.Run(setName+"oldUnmarshal", func(b *testing.B) {
			var out []row.ExampleStruct
			for i := 0; i < b.N; i++ {
				out = oldUnmarshal(data, rows)
			}
			if len(out) == 0 {
				b.Fatal("oldUnmarshal fail")
			}
		})
		b.Run(setName+"newUnmarshal", func(b *testing.B) {
			var out []row.ExampleStruct
			for i := 0; i < b.N; i++ {
				out = newUnmarshal(data, rows)
			}
			if len(out) == 0 {
				b.Fatal("newUnmarshal fail")
			}
		})
	})
}

func oldUnmarshal(data []byte, rows int) []row.ExampleStruct {
	var err error
	var valLen int32
	out := make([]row.ExampleStruct, rows)
	rowData := [3][]byte{}
	columns := []TypeInfo{NativeType{typ: TypeInt}, NativeType{typ: TypeInt}, NativeType{typ: TypeInt}}
	for idx := 0; idx < rows; idx++ {
		for col := 0; col < 3; col++ {
			valLen = dec4ToInt32(data)
			if valLen > 0 {
				rowData[col] = data[4:8]
				data = data[8:]
			} else {
				rowData[col] = []byte{}
				data = data[4:]
			}
		}
		err = Unmarshal(columns[0], rowData[0], &out[idx].Val0)
		if err != nil {
			return nil
		}
		err = Unmarshal(columns[1], rowData[1], &out[idx].Val1)
		if err != nil {
			return nil
		}
		err = Unmarshal(columns[2], rowData[2], &out[idx].Val2)
		if err != nil {
			return nil
		}
	}
	return out
}

func newUnmarshal(data []byte, rows int) []row.ExampleStruct {
	rowSlice := row.ExampleSlice{}
	writer := row.InitToSliceWriter(&rowSlice, 3)
	writer.Prepare(int32(rows))
	write, err := writer.WriteRows(data)
	if err != nil {
		return nil
	}
	if int(write) != len(data) {
		panic("wrong write work")
	}
	return rowSlice.Rows
}

func randInts(rows, columns int) []byte {
	out := make([]byte, 0, rows*columns*8)
	for i := 0; i < rows*3; i++ {
		out = append(out, randInt32(rnd, 2147483647, -2147483648)...)
	}
	return out
}

func randInt32(rnd *rand.Rand, max, min int32) []byte {
	t := rnd.Int31n(4)
	if t == 0 {
		return []byte{255, 255, 255, 255}
	}
	if t == 1 {
		return []byte{0, 0, 0, 0}
	}
	if max == min {
		t = max
	} else {
		if min < 0 {
			t = rnd.Int31n(max) + min
		} else {
			t = rnd.Int31n(max-min) + min
		}
	}
	return append([]byte{0, 0, 0, 4}, []byte{byte(t >> 24), byte(t >> 16), byte(t >> 8), byte(t)}...)
}

func dec4ToInt32(data []byte) int32 {
	return int32(data[0])<<24 | int32(data[1])<<16 | int32(data[2])<<8 | int32(data[3])
}

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
