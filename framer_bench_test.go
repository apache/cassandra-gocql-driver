// Copyright 2019 Gocql Owners

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gocql

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"
)

func readGzipData(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func BenchmarkParseRowsFrame(b *testing.B) {
	data, err := readGzipData("testdata/frames/bench_parse_result.gz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		framer := &framer{
			header: &frameHeader{
				version: protoVersion4 | 0x80,
				op:      opResult,
				length:  len(data),
			},
			rbuf: data,
		}

		_, err = framer.parseFrame()
		if err != nil {
			b.Fatal(err)
		}
	}
}
