package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"math/bits"
	"net"
	"reflect"
	"strconv"
	"time"
)

var (
	bigOne     = big.NewInt(1)
	EmptyValue reflect.Value
)

const MillisecondsInADay int64 = 24 * 60 * 60 * 1000

func EncInt(x int32) []byte {
	return []byte{byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
}

func DecInt(x []byte) int32 {
	if len(x) != 4 {
		return 0
	}
	return int32(x[0])<<24 | int32(x[1])<<16 | int32(x[2])<<8 | int32(x[3])
}

func EncShort(x int16) []byte {
	p := make([]byte, 2)
	p[0] = byte(x >> 8)
	p[1] = byte(x)
	return p
}

func DecShort(p []byte) int16 {
	if len(p) != 2 {
		return 0
	}
	return int16(p[0])<<8 | int16(p[1])
}

func DecTiny(p []byte) int8 {
	if len(p) != 1 {
		return 0
	}
	return int8(p[0])
}

func EncBigInt(x int64) []byte {
	return []byte{byte(x >> 56), byte(x >> 48), byte(x >> 40), byte(x >> 32),
		byte(x >> 24), byte(x >> 16), byte(x >> 8), byte(x)}
}

func BytesToInt64(data []byte) (ret int64) {
	for i := range data {
		ret |= int64(data[i]) << (8 * uint(len(data)-i-1))
	}
	return ret
}

func BytesToUint64(data []byte) (ret uint64) {
	for i := range data {
		ret |= uint64(data[i]) << (8 * uint(len(data)-i-1))
	}
	return ret
}

func DecBigInt(data []byte) int64 {
	if len(data) != 8 {
		return 0
	}
	return int64(data[0])<<56 | int64(data[1])<<48 |
		int64(data[2])<<40 | int64(data[3])<<32 |
		int64(data[4])<<24 | int64(data[5])<<16 |
		int64(data[6])<<8 | int64(data[7])
}

func EncBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func DecBool(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	return v[0] != 0
}

// decBigInt2C sets the value of n to the big-endian two's complement
// value stored in the given data. If data[0]&80 != 0, the number
// is negative. If data is empty, the result will be 0.
func DecBigInt2C(data []byte, n *big.Int) *big.Int {
	if n == nil {
		n = new(big.Int)
	}
	n.SetBytes(data)
	if len(data) > 0 && data[0]&0x80 > 0 {
		n.Sub(n, new(big.Int).Lsh(bigOne, uint(len(data))*8))
	}
	return n
}

// EncBigInt2C returns the big-endian two's complement
// form of n.
func EncBigInt2C(n *big.Int) []byte {
	switch n.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b
	case -1:
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(bigOne, length)).Bytes()
		// When the most significant bit is on a byte
		// boundary, we can get some extra significant
		// bits, so strip them off when that happens.
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b
	}
	return nil
}

func DecVints(data []byte) (int32, int32, int64, error) {
	month, i, err := DecVint(data, 0)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to extract month: %s", err.Error())
	}
	days, i, err := DecVint(data, i)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to extract days: %s", err.Error())
	}
	nanos, _, err := DecVint(data, i)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to extract nanoseconds: %s", err.Error())
	}
	return int32(month), int32(days), nanos, err
}

func DecVint(data []byte, start int) (int64, int, error) {
	if len(data) <= start {
		return 0, 0, errors.New("unexpected eof")
	}
	firstByte := data[start]
	if firstByte&0x80 == 0 {
		return decIntZigZag(uint64(firstByte)), start + 1, nil
	}
	numBytes := bits.LeadingZeros32(uint32(^firstByte)) - 24
	ret := uint64(firstByte & (0xff >> uint(numBytes)))
	if len(data) < start+numBytes+1 {
		return 0, 0, fmt.Errorf("data expect to have %d bytes, but it has only %d", start+numBytes+1, len(data))
	}
	for i := start; i < start+numBytes; i++ {
		ret <<= 8
		ret |= uint64(data[i+1] & 0xff)
	}
	return decIntZigZag(ret), start + numBytes + 1, nil
}

func decIntZigZag(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

func encIntZigZag(n int64) uint64 {
	return uint64((n >> 63) ^ (n << 1))
}

func EncVints(months int32, seconds int32, nanos int64) []byte {
	buf := append(EncVint(int64(months)), EncVint(int64(seconds))...)
	return append(buf, EncVint(nanos)...)
}

func EncVint(v int64) []byte {
	vEnc := encIntZigZag(v)
	lead0 := bits.LeadingZeros64(vEnc)
	numBytes := (639 - lead0*9) >> 6

	// It can be 1 or 0 is v ==0
	if numBytes <= 1 {
		return []byte{byte(vEnc)}
	}
	extraBytes := numBytes - 1
	var buf = make([]byte, numBytes)
	for i := extraBytes; i >= 0; i-- {
		buf[i] = byte(vEnc)
		vEnc >>= 8
	}
	buf[0] |= byte(^(0xff >> uint(extraBytes)))
	return buf
}

// TODO: move to internal
func ReadBytes(p []byte) ([]byte, []byte) {
	// TODO: really should use a framer
	size := ReadInt(p)
	p = p[4:]
	if size < 0 {
		return nil, p
	}
	return p[:size], p[size:]
}

func MarshalVarchar(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	switch {
	case k == reflect.String:
		return []byte(rv.String()), nil
	case k == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return rv.Bytes(), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalBool(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case bool:
		return EncBool(v), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Bool:
		return EncBool(rv.Bool()), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalTinyInt(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int8:
		return []byte{byte(v)}, nil
	case uint8:
		return []byte{byte(v)}, nil
	case int16:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint16:
		if v > math.MaxUint8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case int:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case int32:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case int64:
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint:
		if v > math.MaxUint8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint32:
		if v > math.MaxUint8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case uint64:
		if v > math.MaxUint8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("can not marshal %T into %s: %v", value, info, err)
		}
		return []byte{byte(n)}, nil
	}

	if value == nil {
		return nil, nil
	}

	switch rv := reflect.ValueOf(value); rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		if v > math.MaxInt8 || v < math.MinInt8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxUint8 {
			return nil, fmt.Errorf("marshal tinyint: value %d out of range", v)
		}
		return []byte{byte(v)}, nil
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
	}

	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalSmallInt(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int16:
		return EncShort(v), nil
	case uint16:
		return EncShort(int16(v)), nil
	case int8:
		return EncShort(int16(v)), nil
	case uint8:
		return EncShort(int16(v)), nil
	case int:
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case int32:
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case int64:
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case uint:
		if v > math.MaxUint16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case uint32:
		if v > math.MaxUint16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case uint64:
		if v > math.MaxUint16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case string:
		n, err := strconv.ParseInt(v, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("can not marshal %T into %s: %v", value, info, err)
		}
		return EncShort(int16(n)), nil
	}

	if value == nil {
		return nil, nil
	}

	switch rv := reflect.ValueOf(value); rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		if v > math.MaxInt16 || v < math.MinInt16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxUint16 {
			return nil, fmt.Errorf("marshal smallint: value %d out of range", v)
		}
		return EncShort(int16(v)), nil
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
	}

	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalInt(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, fmt.Errorf("marshal int: value %d out of range", v)
		}
		return EncInt(int32(v)), nil
	case uint:
		if v > math.MaxUint32 {
			return nil, fmt.Errorf("marshal int: value %d out of range", v)
		}
		return EncInt(int32(v)), nil
	case int64:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, fmt.Errorf("marshal int: value %d out of range", v)
		}
		return EncInt(int32(v)), nil
	case uint64:
		if v > math.MaxUint32 {
			return nil, fmt.Errorf("marshal int: value %d out of range", v)
		}
		return EncInt(int32(v)), nil
	case int32:
		return EncInt(v), nil
	case uint32:
		return EncInt(int32(v)), nil
	case int16:
		return EncInt(int32(v)), nil
	case uint16:
		return EncInt(int32(v)), nil
	case int8:
		return EncInt(int32(v)), nil
	case uint8:
		return EncInt(int32(v)), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can not marshal string to int: %s", err)
		}
		return EncInt(int32(i)), nil
	}

	if value == nil {
		return nil, nil
	}

	switch rv := reflect.ValueOf(value); rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		if v > math.MaxInt32 || v < math.MinInt32 {
			return nil, fmt.Errorf("marshal int: value %d out of range", v)
		}
		return EncInt(int32(v)), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxInt32 {
			return nil, fmt.Errorf("marshal int: value %d out of range", v)
		}
		return EncInt(int32(v)), nil
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, nil
		}
	}

	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalBigInt(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int:
		return EncBigInt(int64(v)), nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return nil, fmt.Errorf("marshal bigint: value %d out of range", v)
		}
		return EncBigInt(int64(v)), nil
	case int64:
		return EncBigInt(v), nil
	case uint64:
		return EncBigInt(int64(v)), nil
	case int32:
		return EncBigInt(int64(v)), nil
	case uint32:
		return EncBigInt(int64(v)), nil
	case int16:
		return EncBigInt(int64(v)), nil
	case uint16:
		return EncBigInt(int64(v)), nil
	case int8:
		return EncBigInt(int64(v)), nil
	case uint8:
		return EncBigInt(int64(v)), nil
	case big.Int:
		return EncBigInt2C(&v), nil
	case string:
		i, err := strconv.ParseInt(value.(string), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can not marshal string to bigint: %s", err)
		}
		return EncBigInt(i), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		v := rv.Int()
		return EncBigInt(v), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		v := rv.Uint()
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("marshal bigint: value %d out of range", v)
		}
		return EncBigInt(int64(v)), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalFloat(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case float32:
		return EncInt(int32(math.Float32bits(v))), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Float32:
		return EncInt(int32(math.Float32bits(float32(rv.Float())))), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalDouble(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case float64:
		return EncBigInt(int64(math.Float64bits(v))), nil
	}
	if value == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Float64:
		return EncBigInt(int64(math.Float64bits(rv.Float()))), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalDecimal(info, value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case inf.Dec:
		unscaled := EncBigInt2C(v.UnscaledBig())
		if unscaled == nil {
			return nil, fmt.Errorf("can not marshal %T into %s", value, info)
		}

		buf := make([]byte, 4+len(unscaled))
		copy(buf[0:4], EncInt(int32(v.Scale())))
		copy(buf[4:], unscaled)
		return buf, nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalTime(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int64:
		return EncBigInt(v), nil
	case time.Duration:
		return EncBigInt(v.Nanoseconds()), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return EncBigInt(rv.Int()), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalTimestamp(info, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int64:
		return EncBigInt(v), nil
	case time.Time:
		if v.IsZero() {
			return []byte{}, nil
		}
		x := int64(v.UTC().Unix()*1e3) + int64(v.UTC().Nanosecond()/1e6)
		return EncBigInt(x), nil
	}

	if value == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Type().Kind() {
	case reflect.Int64:
		return EncBigInt(rv.Int()), nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

func MarshalVarint(info, value interface{}) ([]byte, error) {
	var (
		retBytes []byte
		err      error
	)

	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case uint64:
		if v > uint64(math.MaxInt64) {
			retBytes = make([]byte, 9)
			binary.BigEndian.PutUint64(retBytes[1:], v)
		} else {
			retBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(retBytes, v)
		}
	default:
		retBytes, err = MarshalBigInt(info, value)
	}

	if err == nil {
		// trim down to most significant byte
		i := 0
		for ; i < len(retBytes)-1; i++ {
			b0 := retBytes[i]
			if b0 != 0 && b0 != 0xFF {
				break
			}

			b1 := retBytes[i+1]
			if b0 == 0 && b1 != 0 {
				if b1&0x80 == 0 {
					i++
				}
				break
			}

			if b0 == 0xFF && b1 != 0xFF {
				if b1&0x80 > 0 {
					i++
				}
				break
			}
		}
		retBytes = retBytes[i:]
	}

	return retBytes, err
}

func MarshalInet(info, value interface{}) ([]byte, error) {
	// we return either the 4 or 16 byte representation of an
	// ip address here otherwise the db value will be prefixed
	// with the remaining byte values e.g. ::ffff:127.0.0.1 and not 127.0.0.1
	switch val := value.(type) {
	case UnsetColumn:
		return nil, nil
	case net.IP:
		t := val.To4()
		if t == nil {
			return val.To16(), nil
		}
		return t, nil
	case string:
		b := net.ParseIP(val)
		if b != nil {
			t := b.To4()
			if t == nil {
				return b.To16(), nil
			}
			return t, nil
		}
		return nil, fmt.Errorf("cannot marshal. invalid ip string %s", val)
	}

	if value == nil {
		return nil, nil
	}

	return nil, fmt.Errorf("cannot marshal %T into %s", value, info)
}

func MarshalDate(info, value interface{}) ([]byte, error) {
	var timestamp int64
	switch v := value.(type) {
	case UnsetColumn:
		return nil, nil
	case int64:
		timestamp = v
		x := timestamp/MillisecondsInADay + int64(1<<31)
		return EncInt(int32(x)), nil
	case time.Time:
		if v.IsZero() {
			return []byte{}, nil
		}
		timestamp = int64(v.UTC().Unix()*1e3) + int64(v.UTC().Nanosecond()/1e6)
		x := timestamp/MillisecondsInADay + int64(1<<31)
		return EncInt(int32(x)), nil
	case *time.Time:
		if v.IsZero() {
			return []byte{}, nil
		}
		timestamp = int64(v.UTC().Unix()*1e3) + int64(v.UTC().Nanosecond()/1e6)
		x := timestamp/MillisecondsInADay + int64(1<<31)
		return EncInt(int32(x)), nil
	case string:
		if v == "" {
			return []byte{}, nil
		}
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return nil, fmt.Errorf("can not marshal %T into %s, date layout must be '2006-01-02'", value, info)
		}
		timestamp = int64(t.UTC().Unix()*1e3) + int64(t.UTC().Nanosecond()/1e6)
		x := timestamp/MillisecondsInADay + int64(1<<31)
		return EncInt(int32(x)), nil
	}

	if value == nil {
		return nil, nil
	}
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}
