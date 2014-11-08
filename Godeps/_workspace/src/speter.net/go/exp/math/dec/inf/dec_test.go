package inf

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"strings"
	"testing"
)

type decFunZZ func(z, x, y *Dec) *Dec
type decArgZZ struct {
	z, x, y *Dec
}

var decSumZZ = []decArgZZ{
	{NewDec(0, 0), NewDec(0, 0), NewDec(0, 0)},
	{NewDec(1, 0), NewDec(1, 0), NewDec(0, 0)},
	{NewDec(1111111110, 0), NewDec(123456789, 0), NewDec(987654321, 0)},
	{NewDec(-1, 0), NewDec(-1, 0), NewDec(0, 0)},
	{NewDec(864197532, 0), NewDec(-123456789, 0), NewDec(987654321, 0)},
	{NewDec(-1111111110, 0), NewDec(-123456789, 0), NewDec(-987654321, 0)},
	{NewDec(12, 2), NewDec(1, 1), NewDec(2, 2)},
}

var decProdZZ = []decArgZZ{
	{NewDec(0, 0), NewDec(0, 0), NewDec(0, 0)},
	{NewDec(0, 0), NewDec(1, 0), NewDec(0, 0)},
	{NewDec(1, 0), NewDec(1, 0), NewDec(1, 0)},
	{NewDec(-991*991, 0), NewDec(991, 0), NewDec(-991, 0)},
	{NewDec(2, 3), NewDec(1, 1), NewDec(2, 2)},
	{NewDec(2, -3), NewDec(1, -1), NewDec(2, -2)},
	{NewDec(2, 3), NewDec(1, 1), NewDec(2, 2)},
}

func TestDecSignZ(t *testing.T) {
	var zero Dec
	for _, a := range decSumZZ {
		s := a.z.Sign()
		e := a.z.Cmp(&zero)
		if s != e {
			t.Errorf("got %d; want %d for z = %v", s, e, a.z)
		}
	}
}

func TestDecAbsZ(t *testing.T) {
	var zero Dec
	for _, a := range decSumZZ {
		var z Dec
		z.Abs(a.z)
		var e Dec
		e.Set(a.z)
		if e.Cmp(&zero) < 0 {
			e.Sub(&zero, &e)
		}
		if z.Cmp(&e) != 0 {
			t.Errorf("got z = %v; want %v", z, e)
		}
	}
}

func testDecFunZZ(t *testing.T, msg string, f decFunZZ, a decArgZZ) {
	var z Dec
	f(&z, a.x, a.y)
	if (&z).Cmp(a.z) != 0 {
		t.Errorf("%s%+v\n\tgot z = %v; want %v", msg, a, &z, a.z)
	}
}

func TestDecSumZZ(t *testing.T) {
	AddZZ := func(z, x, y *Dec) *Dec { return z.Add(x, y) }
	SubZZ := func(z, x, y *Dec) *Dec { return z.Sub(x, y) }
	for _, a := range decSumZZ {
		arg := a
		testDecFunZZ(t, "AddZZ", AddZZ, arg)

		arg = decArgZZ{a.z, a.y, a.x}
		testDecFunZZ(t, "AddZZ symmetric", AddZZ, arg)

		arg = decArgZZ{a.x, a.z, a.y}
		testDecFunZZ(t, "SubZZ", SubZZ, arg)

		arg = decArgZZ{a.y, a.z, a.x}
		testDecFunZZ(t, "SubZZ symmetric", SubZZ, arg)
	}
}

func TestDecProdZZ(t *testing.T) {
	MulZZ := func(z, x, y *Dec) *Dec { return z.Mul(x, y) }
	for _, a := range decProdZZ {
		arg := a
		testDecFunZZ(t, "MulZZ", MulZZ, arg)

		arg = decArgZZ{a.z, a.y, a.x}
		testDecFunZZ(t, "MulZZ symmetric", MulZZ, arg)
	}
}

var decQuoRemZZZ = []struct {
	z, x, y  *Dec
	r        *big.Rat
	srA, srB int
}{
	// basic examples
	{NewDec(1, 0), NewDec(2, 0), NewDec(2, 0), big.NewRat(0, 1), 0, 1},
	{NewDec(15, 1), NewDec(3, 0), NewDec(2, 0), big.NewRat(0, 1), 0, 1},
	{NewDec(1, 1), NewDec(1, 0), NewDec(10, 0), big.NewRat(0, 1), 0, 1},
	{NewDec(0, 0), NewDec(2, 0), NewDec(3, 0), big.NewRat(2, 3), 1, 1},
	{NewDec(0, 0), NewDec(2, 0), NewDec(6, 0), big.NewRat(1, 3), 1, 1},
	{NewDec(1, 1), NewDec(2, 0), NewDec(12, 0), big.NewRat(2, 3), 1, 1},

	// examples from the Go Language Specification
	{NewDec(1, 0), NewDec(5, 0), NewDec(3, 0), big.NewRat(2, 3), 1, 1},
	{NewDec(-1, 0), NewDec(-5, 0), NewDec(3, 0), big.NewRat(-2, 3), -1, 1},
	{NewDec(-1, 0), NewDec(5, 0), NewDec(-3, 0), big.NewRat(-2, 3), 1, -1},
	{NewDec(1, 0), NewDec(-5, 0), NewDec(-3, 0), big.NewRat(2, 3), -1, -1},
}

func TestDecQuoRem(t *testing.T) {
	for i, a := range decQuoRemZZZ {
		z, rA, rB := new(Dec), new(big.Int), new(big.Int)
		s := scaleQuoExact{}.Scale(a.x, a.y)
		z.quoRem(a.x, a.y, s, true, rA, rB)
		if a.z.Cmp(z) != 0 || a.r.Cmp(new(big.Rat).SetFrac(rA, rB)) != 0 {
			t.Errorf("#%d QuoRemZZZ got %v, %v, %v; expected %v, %v", i, z, rA, rB, a.z, a.r)
		}
		if a.srA != rA.Sign() || a.srB != rB.Sign() {
			t.Errorf("#%d QuoRemZZZ wrong signs, got %v, %v; expected %v, %v", i, rA.Sign(), rB.Sign(), a.srA, a.srB)
		}
	}
}

var decUnscaledTests = []struct {
	d  *Dec
	u  int64 // ignored when ok == false
	ok bool
}{
	{new(Dec), 0, true},
	{NewDec(-1<<63, 0), -1 << 63, true},
	{NewDec(-(-1<<63 + 1), 0), -(-1<<63 + 1), true},
	{new(Dec).Neg(NewDec(-1<<63, 0)), 0, false},
	{new(Dec).Sub(NewDec(-1<<63, 0), NewDec(1, 0)), 0, false},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), 0, false},
}

func TestDecUnscaled(t *testing.T) {
	for i, tt := range decUnscaledTests {
		u, ok := tt.d.Unscaled()
		if ok != tt.ok {
			t.Errorf("#%d Unscaled: got %v, expected %v", i, ok, tt.ok)
		} else if ok && u != tt.u {
			t.Errorf("#%d Unscaled: got %v, expected %v", i, u, tt.u)
		}
	}
}

var decRoundTests = [...]struct {
	in  *Dec
	s   Scale
	r   Rounder
	exp *Dec
}{
	{NewDec(123424999999999993, 15), 2, RoundHalfUp, NewDec(12342, 2)},
	{NewDec(123425000000000001, 15), 2, RoundHalfUp, NewDec(12343, 2)},
	{NewDec(123424999999999993, 15), 15, RoundHalfUp, NewDec(123424999999999993, 15)},
	{NewDec(123424999999999993, 15), 16, RoundHalfUp, NewDec(1234249999999999930, 16)},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), -1, RoundHalfUp, NewDec(1844674407370955162, -1)},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), -2, RoundHalfUp, NewDec(184467440737095516, -2)},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), -3, RoundHalfUp, NewDec(18446744073709552, -3)},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), -4, RoundHalfUp, NewDec(1844674407370955, -4)},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), -5, RoundHalfUp, NewDec(184467440737096, -5)},
	{NewDecBig(new(big.Int).Lsh(big.NewInt(1), 64), 0), -6, RoundHalfUp, NewDec(18446744073710, -6)},
}

func TestDecRound(t *testing.T) {
	for i, tt := range decRoundTests {
		z := new(Dec).Round(tt.in, tt.s, tt.r)
		if tt.exp.Cmp(z) != 0 {
			t.Errorf("#%d Round got %v; expected %v", i, z, tt.exp)
		}
	}
}

var decStringTests = []struct {
	in     string
	out    string
	val    int64
	scale  Scale // skip SetString if negative
	ok     bool
	scanOk bool
}{
	{in: "", ok: false, scanOk: false},
	{in: "a", ok: false, scanOk: false},
	{in: "z", ok: false, scanOk: false},
	{in: "+", ok: false, scanOk: false},
	{in: "-", ok: false, scanOk: false},
	{in: "g", ok: false, scanOk: false},
	{in: ".", ok: false, scanOk: false},
	{in: ".-0", ok: false, scanOk: false},
	{in: ".+0", ok: false, scanOk: false},
	// Scannable but not SetStringable
	{"0b", "ignored", 0, 0, false, true},
	{"0x", "ignored", 0, 0, false, true},
	{"0xg", "ignored", 0, 0, false, true},
	{"0.0g", "ignored", 0, 1, false, true},
	// examples from godoc for Dec
	{"0", "0", 0, 0, true, true},
	{"0.00", "0.00", 0, 2, true, true},
	{"ignored", "0", 0, -2, true, false},
	{"1", "1", 1, 0, true, true},
	{"1.00", "1.00", 100, 2, true, true},
	{"10", "10", 10, 0, true, true},
	{"ignored", "10", 1, -1, true, false},
	// other tests
	{"+0", "0", 0, 0, true, true},
	{"-0", "0", 0, 0, true, true},
	{"0.0", "0.0", 0, 1, true, true},
	{"0.1", "0.1", 1, 1, true, true},
	{"0.", "0", 0, 0, true, true},
	{"-10", "-10", -1, -1, true, true},
	{"-1", "-1", -1, 0, true, true},
	{"-0.1", "-0.1", -1, 1, true, true},
	{"-0.01", "-0.01", -1, 2, true, true},
	{"+0.", "0", 0, 0, true, true},
	{"-0.", "0", 0, 0, true, true},
	{".0", "0.0", 0, 1, true, true},
	{"+.0", "0.0", 0, 1, true, true},
	{"-.0", "0.0", 0, 1, true, true},
	{"0.0000000000", "0.0000000000", 0, 10, true, true},
	{"0.0000000001", "0.0000000001", 1, 10, true, true},
	{"-0.0000000000", "0.0000000000", 0, 10, true, true},
	{"-0.0000000001", "-0.0000000001", -1, 10, true, true},
	{"-10", "-10", -10, 0, true, true},
	{"+10", "10", 10, 0, true, true},
	{"00", "0", 0, 0, true, true},
	{"023", "23", 23, 0, true, true},      // decimal, not octal
	{"-02.3", "-2.3", -23, 1, true, true}, // decimal, not octal
}

func TestDecGetString(t *testing.T) {
	z := new(Dec)
	for i, test := range decStringTests {
		if !test.ok {
			continue
		}
		z.SetUnscaled(test.val)
		z.SetScale(test.scale)

		s := z.String()
		if s != test.out {
			t.Errorf("#%da got %s; want %s", i, s, test.out)
		}

		s = fmt.Sprintf("%d", z)
		if s != test.out {
			t.Errorf("#%db got %s; want %s", i, s, test.out)
		}
	}
}

func TestDecSetString(t *testing.T) {
	tmp := new(Dec)
	for i, test := range decStringTests {
		if test.scale < 0 {
			// SetString only supports scale >= 0
			continue
		}
		// initialize to a non-zero value so that issues with parsing
		// 0 are detected
		tmp.Set(NewDec(1234567890, 123))
		n1, ok1 := new(Dec).SetString(test.in)
		n2, ok2 := tmp.SetString(test.in)
		expected := NewDec(test.val, test.scale)
		if ok1 != test.ok || ok2 != test.ok {
			t.Errorf("#%d (input '%s') ok incorrect (should be %t)", i, test.in, test.ok)
			continue
		}
		if !ok1 {
			if n1 != nil {
				t.Errorf("#%d (input '%s') n1 != nil", i, test.in)
			}
			continue
		}
		if !ok2 {
			if n2 != nil {
				t.Errorf("#%d (input '%s') n2 != nil", i, test.in)
			}
			continue
		}

		if n1.Cmp(expected) != 0 {
			t.Errorf("#%d (input '%s') got: %s want: %d", i, test.in, n1, test.val)
		}
		if n2.Cmp(expected) != 0 {
			t.Errorf("#%d (input '%s') got: %s want: %d", i, test.in, n2, test.val)
		}
	}
}

func TestDecScan(t *testing.T) {
	tmp := new(Dec)
	for i, test := range decStringTests {
		if test.scale < 0 {
			// SetString only supports scale >= 0
			continue
		}
		// initialize to a non-zero value so that issues with parsing
		// 0 are detected
		tmp.Set(NewDec(1234567890, 123))
		n1, n2 := new(Dec), tmp
		nn1, err1 := fmt.Sscan(test.in, n1)
		nn2, err2 := fmt.Sscan(test.in, n2)
		if !test.scanOk {
			if err1 == nil || err2 == nil {
				t.Errorf("#%d (input '%s') ok incorrect, should be %t", i, test.in, test.scanOk)
			}
			continue
		}
		expected := NewDec(test.val, test.scale)
		if nn1 != 1 || err1 != nil || nn2 != 1 || err2 != nil {
			t.Errorf("#%d (input '%s') error %d %v, %d %v", i, test.in, nn1, err1, nn2, err2)
			continue
		}
		if n1.Cmp(expected) != 0 {
			t.Errorf("#%d (input '%s') got: %s want: %d", i, test.in, n1, test.val)
		}
		if n2.Cmp(expected) != 0 {
			t.Errorf("#%d (input '%s') got: %s want: %d", i, test.in, n2, test.val)
		}
	}
}

var decScanNextTests = []struct {
	in   string
	ok   bool
	next rune
}{
	{"", false, 0},
	{"a", false, 'a'},
	{"z", false, 'z'},
	{"+", false, 0},
	{"-", false, 0},
	{"g", false, 'g'},
	{".", false, 0},
	{".-0", false, '-'},
	{".+0", false, '+'},
	{"0b", true, 'b'},
	{"0x", true, 'x'},
	{"0xg", true, 'x'},
	{"0.0g", true, 'g'},
}

func TestDecScanNext(t *testing.T) {
	for i, test := range decScanNextTests {
		rdr := strings.NewReader(test.in)
		n1 := new(Dec)
		nn1, _ := fmt.Fscan(rdr, n1)
		if (test.ok && nn1 == 0) || (!test.ok && nn1 > 0) {
			t.Errorf("#%d (input '%s') ok incorrect should be %t", i, test.in, test.ok)
			continue
		}
		r := rune(0)
		nn2, err := fmt.Fscanf(rdr, "%c", &r)
		if test.next != r {
			t.Errorf("#%d (input '%s') next incorrect, got %c should be %c, %d, %v", i, test.in, r, test.next, nn2, err)
		}
	}
}

var decGobEncodingTests = []string{
	"0",
	"1",
	"2",
	"10",
	"42",
	"1234567890",
	"298472983472983471903246121093472394872319615612417471234712061",
}

func TestDecGobEncoding(t *testing.T) {
	var medium bytes.Buffer
	enc := gob.NewEncoder(&medium)
	dec := gob.NewDecoder(&medium)
	for i, test := range decGobEncodingTests {
		for j := 0; j < 2; j++ {
			for k := Scale(-5); k <= 5; k++ {
				medium.Reset() // empty buffer for each test case (in case of failures)
				stest := test
				if j != 0 {
					// negative numbers
					stest = "-" + test
				}
				var tx Dec
				tx.SetString(stest)
				tx.SetScale(k) // test with positive, negative, and zero scale
				if err := enc.Encode(&tx); err != nil {
					t.Errorf("#%d%c: encoding failed: %s", i, 'a'+j, err)
				}
				var rx Dec
				if err := dec.Decode(&rx); err != nil {
					t.Errorf("#%d%c: decoding failed: %s", i, 'a'+j, err)
				}
				if rx.Cmp(&tx) != 0 {
					t.Errorf("#%d%c: transmission failed: got %s want %s", i, 'a'+j, &rx, &tx)
				}
			}
		}
	}
}
