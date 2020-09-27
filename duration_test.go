package gocql

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		arg string
		dur Duration
		err error
	}{
		{arg: "1y", dur: Duration{Months: 12}},
		{arg: "1mo", dur: Duration{Months: 1}},
		{arg: "1w", dur: Duration{Days: 7}},
		{arg: "1d", dur: Duration{Days: 1}},
		{arg: "1h", dur: Duration{Nanoseconds: int64(time.Hour)}},
		{arg: "1m", dur: Duration{Nanoseconds: int64(time.Minute)}},
		{arg: "1s", dur: Duration{Nanoseconds: int64(time.Second)}},
		{arg: "1ms", dur: Duration{Nanoseconds: int64(time.Millisecond)}},
		{arg: "1us", dur: Duration{Nanoseconds: int64(time.Microsecond)}},
		{arg: "1ns", dur: Duration{Nanoseconds: int64(time.Nanosecond)}},

		{
			arg: "1y1mo1w1d1h1m1s1ms1us1ns",
			dur: Duration{
				Months:      13,
				Days:        8,
				Nanoseconds: int64(time.Hour + time.Minute + time.Second + time.Millisecond + time.Microsecond + time.Nanosecond),
			},
		},

		{arg: "-1mo1d1ns", dur: Duration{Months: -1, Days: -1, Nanoseconds: -1}},
	}

	for _, test := range tests {
		dur, err := ParseDuration(test.arg)
		if dur != test.dur {
			t.Errorf("%q want duration: %v, got: %v", test.arg, test.dur, dur)
		}
		if err != test.err {
			t.Errorf("%q want error: %v, got %v", test.arg, test.err, err)
		}
	}
}

func TestDurationToString(t *testing.T) {
	tests := []struct {
		dur Duration
		res string
	}{
		{dur: Duration{}, res: "0s"},

		{dur: Duration{Nanoseconds: 1}, res: "1ns"},
		{dur: Duration{Nanoseconds: 1e3}, res: "1µs"},
		{dur: Duration{Nanoseconds: 1e3 + 1}, res: "1µs1ns"},
		{dur: Duration{Nanoseconds: 1e6}, res: "1ms"},
		{dur: Duration{Nanoseconds: 1e6 + 1}, res: "1ms1ns"},
		{dur: Duration{Nanoseconds: 1e6 + 1e3}, res: "1ms1µs"},
		{dur: Duration{Nanoseconds: 1e6 + 1e3 + 1}, res: "1ms1µs1ns"},
		{dur: Duration{Nanoseconds: 1e9}, res: "1s"},
		{dur: Duration{Nanoseconds: 1e9 + 1}, res: "1s1ns"},
		{dur: Duration{Nanoseconds: 1e9 + 1e3}, res: "1s1µs"},
		{dur: Duration{Nanoseconds: 1e9 + 1e3 + 1}, res: "1s1µs1ns"},
		{dur: Duration{Nanoseconds: 1e9 + 1e6}, res: "1s1ms"},
		{dur: Duration{Nanoseconds: 1e9 + 1e6 + 1}, res: "1s1ms1ns"},
		{dur: Duration{Nanoseconds: 1e9 + 1e6 + 1e3}, res: "1s1ms1µs"},
		{dur: Duration{Nanoseconds: 1e9 + 1e6 + 1e3 + 1}, res: "1s1ms1µs1ns"},

		{dur: Duration{Nanoseconds: 1e9 * 60}, res: "1m"},
		{dur: Duration{Nanoseconds: 1e9 * 61}, res: "1m1s"},
		{dur: Duration{Nanoseconds: 1e9 * 3600}, res: "1h"},
		{dur: Duration{Nanoseconds: 1e9 * 3601}, res: "1h1s"},
		{dur: Duration{Nanoseconds: 1e9 * 3660}, res: "1h1m"},
		{dur: Duration{Nanoseconds: 1e9 * 3661}, res: "1h1m1s"},

		{dur: Duration{Days: 1}, res: "1d"},

		{dur: Duration{Months: 1}, res: "1mo"},
		{dur: Duration{Months: 12}, res: "1y"},
		{dur: Duration{Months: 13}, res: "1y1mo"},

		{dur: Duration{Nanoseconds: -1, Days: -1, Months: -1}, res: "-1mo1d1ns"},

		{dur: Duration{Nanoseconds: -1, Days: -1, Months: 0}, res: "-1d1ns"},
		{dur: Duration{Nanoseconds: -1, Days: -1, Months: 1}, res: "1mo1d1ns"},

		{dur: Duration{Nanoseconds: -1, Days: 0, Months: -1}, res: "-1mo1ns"},
		{dur: Duration{Nanoseconds: -1, Days: 1, Months: -1}, res: "1mo1d1ns"},

		{dur: Duration{Days: -1, Months: -1}, res: "-1mo1d"},
		{dur: Duration{Nanoseconds: 1, Days: -1, Months: -1}, res: "1mo1d1ns"},
	}

	for _, test := range tests {
		res := test.dur.String()
		if res != test.res {
			t.Errorf("expected result %q, got %q", test.res, res)
		}
	}
}
