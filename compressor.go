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
	"github.com/golang/snappy"
)

type Compressor interface {
	Name() string
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. The snappy compression algorithm
// aims for very high speeds and reasonable compression.
type SnappyCompressor struct{}

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) Encode(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s SnappyCompressor) Decode(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
