package murmur

import (
	"testing"
)

// Test the implementation of murmur3
func TestMurmur3H1ForCassandra(t *testing.T) {
	prefix2 := "prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\xc8\xc9"
	expectedHashes := map[string]int64{
		"prefix\x00":                                                           -5156414768376541762,
		"prefix\x00\x01":                                                       3700033067394128583,
		"prefix\x00\x01\x02":                                                   -3973852022020331202,
		"prefix\x00\x01\x02\x03":                                               3038655072509758878,
		"prefix\x00\x01\x02\x03\x04":                                           -5681686890369564299,
		"prefix\x00\x01\x02\x03\x04\x05":                                       8960422816272944721,
		"prefix\x00\x01\x02\x03\x04\x05\x06":                                   -1104872167714568090,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07":                               -9199676315897541990,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08":                           4847624695450664884,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t":                         -2737291859101550625,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n":                       -1154133289533387924,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b":                   5781349077088505981,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c":               -681021187788781932,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r":             -3787130296809111096,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e":         -3115393326357146996,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f":     -2684252762052835206,
		"prefix\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\xc8": -278181328349571586,

		prefix2:                                                              -5319761130493488577,
		prefix2 + "\xca":                                                     -4593148434938707926,
		prefix2 + "\xca\xcb":                                                 -575713103357290760,
		prefix2 + "\xca\xcb\xcc":                                             4290743753678124926,
		prefix2 + "\xca\xcb\xcc\xcd":                                         9164754117279683406,
		prefix2 + "\xca\xcb\xcc\xcd\xce":                                     -107668053986278786,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf":                                 64887291506465003,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0":                             6241282934480161521,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1":                         5150183507841894143,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2":                     5645797256444299552,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3":                 -908235285856554578,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4":             -1785405279637408062,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5":         5343233714768265848,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6":     4936964276934412722,
		prefix2 + "\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7": 2294903531026336836,

		"\x00\x10C'R\x9f\xb6E\xdd\x00\xb8\x83\xec9\xaeD\x8b\xb8\x00\x00\x04\x00\x06jk\x00": -9223371632693506265,
	}

	for s, v := range expectedHashes {
		if CassandraMurmur3H1([]byte(s)) != v {
			t.Log("bad hash:", v, "for:", []byte(s))
			t.Fail()
		}
	}
}

// Benchmark of the performance of the murmur3 implementation
func BenchmarkMurmur3H1(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = CassandraMurmur3H1(data)
		}
	})
}
