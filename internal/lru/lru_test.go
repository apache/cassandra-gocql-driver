/*
Copyright 2015 To gocql authors
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lru

import (
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type simpleStruct struct {
	int
	string
}

type complexStruct struct {
	int
	simpleStruct
}

var getTests = []struct {
	name       string
	keyToAdd   string
	keyToGet   string
	expectedOk bool
}{
	{"string_hit", "mystring", "mystring", true},
	{"string_miss", "mystring", "nonsense", false},
	{"simple_struct_hit", "two", "two", true},
	{"simeple_struct_miss", "two", "noway", false},
}

func TestGet(t *testing.T) {
	for _, tt := range getTests {
		lru := New(0, 0)
		lru.Add(tt.keyToAdd, 1234)
		val, ok := lru.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

var evictTests = []struct {
	name        string
	maxElems    int
	elemsToAdd  int
	expectedLen int
}{
	{"only_one", 10, 1, 1},
	{"just_full", 10, 10, 10},
	{"overflow", 10, 20, 20},
}

func TestAutomaticEvictionNoWindow(t *testing.T) {
	for _, tt := range evictTests {
		lru := New(tt.maxElems, 0)
		for i := 0; i < tt.elemsToAdd; i++ {
			lru.Add(strconv.Itoa(i), i)
		}
		if lru.Len() != tt.expectedLen {
			t.Fatalf("automatic eviction, %s: expected %d got %d", tt.name, tt.expectedLen, lru.Len())
		}
	}
}

func TestAutomaticEvictionWithWindow(t *testing.T) {
	for _, tt := range evictTests {
		lru := New(tt.maxElems, time.Second)
		for i := 0; i < tt.elemsToAdd; i++ {
			lru.Add(strconv.Itoa(i), i)
		}
		if lru.Len() != tt.expectedLen {
			t.Fatalf("automatic eviction, %s: expected %d got %d", tt.name, tt.expectedLen, lru.Len())
		}
	}
}

func TestRemove(t *testing.T) {
	lru := New(0, 0)
	lru.Add("mystring", 1234)
	if val, ok := lru.Get("mystring"); !ok {
		t.Fatal("TestRemove returned no match")
	} else if val != 1234 {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove("mystring")
	if _, ok := lru.Get("mystring"); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

func hammerLRU(lru *Cache, n int64, loops int) {
	for i := 0; i < loops; i++ {
		key := strconv.Itoa(i)
		switch rand.Intn(100) % 7 {
		case 1:
			lru.getWithLock(key, time.Now())
		case 2:
			lru.Get(key)
		case 3:
			lru.Add(key, n)
		case 4:
			lru.Len()
		case 5:
			lru.RemoveOldest()
		case 6:
			lru.Remove(key)
		default:
			lru.GetOrInsert(key, n)
		}
	}
}

func TestStressWithWindow(t *testing.T) {
	t.Parallel()

	n := runtime.GOMAXPROCS(0)
	loops := 1000000 / n
	var wg sync.WaitGroup
	wg.Add(n)

	lru := New(1000, 10*time.Microsecond)
	lru.OnEvicted = func(key string, value interface{}) {}

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			hammerLRU(lru, int64(i), loops)
		}()
	}
	wg.Wait()
}

func TestStressWithoutWindow(t *testing.T) {
	t.Parallel()

	n := runtime.GOMAXPROCS(0)
	loops := 1000000 / n
	var wg sync.WaitGroup
	wg.Add(n)

	lru := New(1000, 0)
	lru.OnEvicted = func(key string, value interface{}) {}

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			hammerLRU(lru, int64(i), loops)
		}()
	}
	wg.Wait()
}
