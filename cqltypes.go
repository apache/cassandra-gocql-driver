// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
)

type Duration struct {
	Months      int32
	Days        int32
	Nanoseconds int64
}

var errLeadingInt = errors.New("bad [0-9]*")

// leadingInt consumes the leading [0-9]* from s.
func leadingInt(s string) (x int64, rem string, err error) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > (1<<63-1)/10 {
			// overflow
			return 0, "", errLeadingInt
		}
		x = x*10 + int64(c) - '0'
		if x < 0 {
			// overflow
			return 0, "", errLeadingInt
		}
	}
	return x, s[i:], nil
}

var unitMap = map[string]int64{
	"ns": 1,
	"us": 1e3,
	"µs": 1e3, // U+00B5 = micro symbol
	"μs": 1e3, // U+03BC = Greek letter mu
	"ms": 1e6,
	"s":  1e9,
	"m":  6 * 1e10,
	"h":  36 * 1e11,
	// units below cannot be expressed in nanoseconds
	"d":  0,
	"w":  0,
	"mo": 0,
	"y":  0,
}

// ParseDuration parses a duration string.
// A duration string is a possibly signed sequence of
// decimal numbers, each with a unit suffix, such as
// "1y1mo1w1d" or "-2h30ns".
// Unlike time.ParseDuration, this function doesn't
// handle fractions, and will return an error parsing strings containing them.
// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h", "d", "w", "mo", "y".
func ParseDuration(s string) (Duration, error) {
	orig := s
	var (
		m  int32
		d  int32
		ns int64
	)
	neg := false

	// Consume [-+]?
	if s != "" {
		c := s[0]
		if c == '-' || c == '+' {
			neg = c == '-'
			s = s[1:]
		}
	}
	if s == "" {
		return Duration{}, errors.New("gocql: invalid duration " + orig)
	}
	for s != "" {
		var v int64 // integer value
		var err error

		// The next character must be [0-9.]
		if !('0' <= s[0] && s[0] <= '9') {
			return Duration{}, errors.New("gocql: invalid duration " + orig)
		}

		// Consume [0-9]*
		pl := len(s)
		v, s, err = leadingInt(s)
		if err != nil {
			return Duration{}, errors.New("gocql: invalid duration " + orig)
		}
		if pl == len(s) {
			// no digits (e.g. "y")
			return Duration{}, errors.New("gocql: invalid duration " + orig)
		}

		// Consume unit.
		i := 0
		for ; i < len(s); i++ {
			c := s[i]
			if '0' <= c && c <= '9' {
				break
			}
		}
		if i == 0 {
			return Duration{}, errors.New("gocql: missing unit in duration " + orig)
		}
		u := s[:i]
		s = s[i:]
		unit, ok := unitMap[u]
		if !ok {
			return Duration{}, errors.New("gocql: unknown unit " + u + " in duration " + orig)
		}
		if u == "y" {
			if v > (1<<31-1)/12 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
			v *= 12
			m += int32(v)
			if m < 0 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
		} else if u == "mo" {
			if v > (1<<31 - 1) {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
			m += int32(v)
			if m < 0 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
		} else if u == "w" {
			if v > (1<<31-1)/7 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
			v *= 7
			d += int32(v)
			if d < 0 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
		} else if u == "d" {
			if v > (1<<31 - 1) {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
			d += int32(v)
			if d < 0 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
		} else {
			if v > (1<<63-1)/unit {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
			v *= unit
			ns += v
			if ns < 0 {
				// overflow
				return Duration{}, errors.New("gocql: invalid duration " + orig)
			}
		}
	}

	if neg {
		m = -m
		d = -d
		ns = -ns
	}
	return Duration{Months: m, Days: d, Nanoseconds: ns}, nil
}

func (d Duration) String() string {
	// Largest time is 178956970y7mo2147483647d2540400h10m10s
	var buf [64]byte
	w := len(buf)

	uns := uint64(d.Nanoseconds)
	negns := d.Nanoseconds < 0
	if negns {
		uns = -uns
	}

	ud := uint32(d.Days)
	negd := d.Days < 0
	if negd {
		ud = -ud
	}

	umo := uint32(d.Months)
	negmo := d.Months < 0
	if negmo {
		umo = -umo
	}

	if uns == 0 {
		if ud == 0 && umo == 0 {
			return "0s"
		}
	} else {
		// consume sub-seconds
		uss := uns % 1e9
		var unit int
		for uss > 0 {
			du := uss % 1000
			if du != 0 {
				switch unit {
				case 0:
					w -= 2
					copy(buf[w:], "ns")
				case 1:
					w -= 3
					copy(buf[w:], "µs")
				case 2:
					w -= 2
					copy(buf[w:], "ms")
				}
				w = fmtInt(buf[:w], du)
			}
			uss /= 1000
			unit++
		}
		// consume seconds
		us := uns / 1e9
		s := us % 60
		if s > 0 {
			w--
			buf[w] = 's'
			w = fmtInt(buf[:w], s)
		}
		// consume minutes
		us /= 60
		m := us % 60
		if m > 0 {
			w--
			buf[w] = 'm'
			w = fmtInt(buf[:w], m)
		}
		// consume hours
		us /= 60
		h := us % 60
		if h > 0 {
			w--
			buf[w] = 'h'
			w = fmtInt(buf[:w], h)
		}
	}
	// consume days
	if ud > 0 {
		w--
		buf[w] = 'd'
		w = fmtInt(buf[:w], uint64(ud))
	}
	//consume months
	if umo > 0 {
		y := umo / 12
		umo = umo % 12
		if umo > 0 {
			w -= 2
			copy(buf[w:], "mo")
			w = fmtInt(buf[:w], uint64(umo))
		}

		if y > 0 {
			w--
			buf[w] = 'y'
			w = fmtInt(buf[:w], uint64(y))
		}
	}

	// assume negative iff all non-zero values are negative
	neg := negns && (negd || ud == 0) && (negmo || umo == 0) ||
		negd && (negns || uns == 0) && (negmo || umo == 0) ||
		negmo && (negns || uns == 0) && (negd || ud == 0)

	if neg {
		w--
		buf[w] = '-'
	}

	return string(buf[w:])
}

// fmtInt formats v into the tail of buf.
// It returns the index where the output begins.
func fmtInt(buf []byte, v uint64) int {
	w := len(buf)
	for v > 0 {
		w--
		buf[w] = byte(v%10) + '0'
		v /= 10
	}
	return w
}
