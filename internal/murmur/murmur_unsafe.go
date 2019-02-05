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

// +build !appengine

package murmur

import (
	"unsafe"
)

func getBlock(data []byte, n int) (int64, int64) {
	block := (*[2]int64)(unsafe.Pointer(&data[n*16]))

	k1 := block[0]
	k2 := block[1]
	return k1, k2
}
