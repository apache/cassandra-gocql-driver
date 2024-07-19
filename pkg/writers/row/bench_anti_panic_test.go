// nolint
package row

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkAntiPanicOrCheckLen_test(b *testing.B) {
	runAntiPanicSet(500000, b)
	runAntiPanicSet(500, b)
	runAntiPanicSet(50, b)
}

func runAntiPanicSet(bytesLen int, b *testing.B) {
	setName := fmt.Sprintf("framer len %d", bytesLen)
	data := randBytes(bytesLen)
	b.Run(setName, func(b *testing.B) {
		fmt.Println("--------------------------------------", setName, "---------------------------------------")
		runReadWith("          read2If4", read2If4, data, b)
		runReadWith("read2WithAntiPanic", read2WithAntiPanic, data, b)
		runReadWith("     read4WithIf10", read4WithIf10, data, b)
		runReadWith("read4WithAntiPanic", read4WithAntiPanic, data, b)
	})
}

func runReadWith(name string, rFunc func([]byte), data []byte, b *testing.B) {
	b.Run(name, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rFunc(data)
		}
	})
}

//go:noinline
func read2If4(data []byte) {
	elems := len(data) / 4
	tmp := int16(0)
	var err error
	for idx := 0; idx < elems; idx++ {
		tmp, err = dec2ToInt16Err(data)
		if err != nil {
			return
		}
		data = data[2:]
		tmp, err = dec2ToInt16Err(data)
		if err != nil {
			return
		}
		data = data[2:]
		_ = tmp
	}
}

//go:noinline
func read2WithAntiPanic(data []byte) {
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%s", errF), "runtime error: index out of range") {
				return
			}
		}
	}()
	elems := len(data) / 4
	tmp := int16(0)
	for idx := 0; idx < elems; idx++ {
		tmp = dec2ToInt16(data)
		data = data[2:]
		tmp = dec2ToInt16(data)
		data = data[2:]
		_ = tmp
	}
}

//go:noinline
func read4WithIf10(data []byte) {
	elems := len(data) / 4
	tmp := int16(0)
	var err error
	for idx := 0; idx < elems; idx++ {
		tmp, err = twiceDec2ToInt16Err(data)
		if err != nil {
			return
		}
		data = data[4:]
		tmp, err = twiceDec2ToInt16Err(data)
		if err != nil {
			return
		}
		data = data[4:]
		_ = tmp
	}
}

//go:noinline
func read4WithAntiPanic(data []byte) {
	defer func() {
		if errF := recover(); errF != nil {
			if strings.Contains(fmt.Sprintf("%s", errF), "runtime error: index out of range") {
				return
			}
		}
	}()
	elems := len(data) / 8
	tmp := int16(0)
	for idx := 0; idx < elems; idx++ {
		tmp = twiceDec2ToInt16(data)
		data = data[4:]
		tmp = twiceDec2ToInt16(data)
		data = data[4:]
		_ = tmp
	}
}

func twiceDec2ToInt16Err(data []byte) (int16, error) {
	tmp, err := dec2ToInt16Err(data)
	if err != nil {
		return 0, err
	}
	data = data[2:]
	tmp, err = dec2ToInt16Err(data)
	if err != nil {
		return 0, err
	}
	data = data[2:]
	return tmp, nil
}

func twiceDec2ToInt16(data []byte) int16 {
	tmp := dec2ToInt16(data)
	data = data[2:]
	tmp = dec2ToInt16(data)
	data = data[2:]
	return tmp
}
