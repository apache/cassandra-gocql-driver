package protocol

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Duration struct {
	Months      int32
	Days        int32
	Nanoseconds int64
}

const (
	VariantNCSCompat = 0
	VariantIETF      = 2
	VariantMicrosoft = 6
	VariantFuture    = 7
)

// ParseUUID parses a 32 digit hexadecimal number (that might contain hypens)
// representing an UUID.
func ParseUUID(input string) (UUID, error) {
	var u UUID
	j := 0
	for _, r := range input {
		switch {
		case r == '-' && j&1 == 0:
			continue
		case r >= '0' && r <= '9' && j < 32:
			u[j/2] |= byte(r-'0') << uint(4-j&1*4)
		case r >= 'a' && r <= 'f' && j < 32:
			u[j/2] |= byte(r-'a'+10) << uint(4-j&1*4)
		case r >= 'A' && r <= 'F' && j < 32:
			u[j/2] |= byte(r-'A'+10) << uint(4-j&1*4)
		default:
			return UUID{}, fmt.Errorf("invalid UUID %q", input)
		}
		j += 1
	}
	if j != 32 {
		return UUID{}, fmt.Errorf("invalid UUID %q", input)
	}
	return u, nil
}

// UUIDFromBytes converts a raw byte slice to an UUID.
func UUIDFromBytes(input []byte) (UUID, error) {
	var u UUID
	if len(input) != 16 {
		return u, errors.New("UUIDs must be exactly 16 bytes long")
	}

	copy(u[:], input)
	return u, nil
}

type UUID [16]byte

var TimeBase = time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC).Unix()

func (u UUID) String() string {
	var offsets = [...]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}
	const hexString = "0123456789abcdef"
	r := make([]byte, 36)
	for i, b := range u {
		r[offsets[i]] = hexString[b>>4]
		r[offsets[i]+1] = hexString[b&0xF]
	}
	r[8] = '-'
	r[13] = '-'
	r[18] = '-'
	r[23] = '-'
	return string(r)

}

// Bytes returns the raw byte slice for this UUID. A UUID is always 128 bits
// (16 bytes) long.
func (u UUID) Bytes() []byte {
	return u[:]
}

// Variant returns the variant of this UUID. This package will only generate
// UUIDs in the IETF variant.
func (u UUID) Variant() int {
	x := u[8]
	if x&0x80 == 0 {
		return VariantNCSCompat
	}
	if x&0x40 == 0 {
		return VariantIETF
	}
	if x&0x20 == 0 {
		return VariantMicrosoft
	}
	return VariantFuture
}

// Version extracts the version of this UUID variant. The RFC 4122 describes
// five kinds of UUIDs.
func (u UUID) Version() int {
	return int(u[6] & 0xF0 >> 4)
}

// Node extracts the MAC address of the node who generated this UUID. It will
// return nil if the UUID is not a time based UUID (version 1).
func (u UUID) Node() []byte {
	if u.Version() != 1 {
		return nil
	}
	return u[10:]
}

// Clock extracts the clock sequence of this UUID. It will return zero if the
// UUID is not a time based UUID (version 1).
func (u UUID) Clock() uint32 {
	if u.Version() != 1 {
		return 0
	}

	// Clock sequence is the lower 14bits of u[8:10]
	return uint32(u[8]&0x3F)<<8 | uint32(u[9])
}

// Timestamp extracts the timestamp information from a time based UUID
// (version 1).
func (u UUID) Timestamp() int64 {
	if u.Version() != 1 {
		return 0
	}
	return int64(uint64(u[0])<<24|uint64(u[1])<<16|
		uint64(u[2])<<8|uint64(u[3])) +
		int64(uint64(u[4])<<40|uint64(u[5])<<32) +
		int64(uint64(u[6]&0x0F)<<56|uint64(u[7])<<48)
}

// Time is like Timestamp, except that it returns a time.Time.
func (u UUID) Time() time.Time {
	if u.Version() != 1 {
		return time.Time{}
	}
	t := u.Timestamp()
	sec := t / 1e7
	nsec := (t % 1e7) * 100
	return time.Unix(sec+TimeBase, nsec).UTC()
}

// Marshaling for JSON
func (u UUID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + u.String() + `"`), nil
}

// Unmarshaling for JSON
func (u *UUID) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)
	if len(str) > 36 {
		return fmt.Errorf("invalid JSON UUID %s", str)
	}

	parsed, err := ParseUUID(str)
	if err == nil {
		copy(u[:], parsed[:])
	}

	return err
}

func (u UUID) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

func (u *UUID) UnmarshalText(text []byte) (err error) {
	*u, err = ParseUUID(string(text))
	return
}
