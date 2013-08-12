// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestServer struct {
	Address string
	t       *testing.T
	nreq    uint64
	listen  net.Listener
}

func NewTestServer(t *testing.T, address string) *TestServer {
	listen, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	srv := &TestServer{Address: address, listen: listen, t: t}
	go srv.serve()
	return srv
}

func (srv *TestServer) serve() {
	defer srv.listen.Close()
	for {
		conn, err := srv.listen.Accept()
		if err != nil {
			break
		}
		go func(conn net.Conn) {
			defer conn.Close()
			for {
				frame := srv.readFrame(conn)
				atomic.AddUint64(&srv.nreq, 1)
				srv.process(frame, conn)
			}
		}(conn)
	}
}

func (srv *TestServer) Stop() {
	srv.listen.Close()
}

func (srv *TestServer) process(frame frame, conn net.Conn) {
	switch frame[3] {
	case opStartup:
		frame = frame[:headerSize]
		frame.setHeader(protoResponse, 0, frame[2], opReady)
	case opQuery:
		input := frame
		input.skipHeader()
		query := strings.TrimSpace(input.readLongString())
		frame = frame[:headerSize]
		frame.setHeader(protoResponse, 0, frame[2], opResult)
		first := query
		if n := strings.IndexByte(query, ' '); n > 0 {
			first = first[:n]
		}
		switch strings.ToLower(first) {
		case "kill":
			select {}
		case "slow":
			go func() {
				<-time.After(1 * time.Second)
				frame.writeInt(0)
				frame.setLength(len(frame) - headerSize)
				if _, err := conn.Write(frame); err != nil {
					return
				}
			}()
			return
		case "use":
			frame.writeInt(3)
			frame.writeString(strings.TrimSpace(query[3:]))
		case "void":
			frame.writeInt(0)
		default:
			frame.writeInt(0)
		}
	default:
		frame = frame[:headerSize]
		frame.setHeader(protoResponse, 0, frame[2], opError)
		frame.writeInt(0)
		frame.writeString("not supported")
	}
	frame.setLength(len(frame) - headerSize)
	if _, err := conn.Write(frame); err != nil {
		return
	}
}

func (srv *TestServer) readFrame(conn net.Conn) frame {
	frame := make(frame, headerSize, headerSize+512)
	if _, err := io.ReadFull(conn, frame); err != nil {
		srv.t.Fatal(err)
	}
	if n := frame.Length(); n > 0 {
		frame.grow(n)
		if _, err := io.ReadFull(conn, frame[headerSize:]); err != nil {
			srv.t.Fatal(err)
		}
	}
	return frame
}

func TestSimple(t *testing.T) {
	srv := NewTestServer(t, "127.0.0.1:9051")
	defer srv.Stop()

	db := NewSession(Config{
		Nodes:       []string{srv.Address},
		Consistency: ConQuorum,
	})
	if err := db.Query("void").Exec(); err != nil {
		t.Error(err)
	}
}

func TestTimeout(t *testing.T) {
	srv := NewTestServer(t, "127.0.0.1:9051")
	defer srv.Stop()

	db := NewSession(Config{
		Nodes:       []string{srv.Address},
		Consistency: ConQuorum,
	})

	go func() {
		<-time.After(1 * time.Second)
		t.Fatal("no timeout")
	}()

	if err := db.Query("kill").Exec(); err == nil {
		t.Fatal("expected error")
	}
}

func TestSlowQuery(t *testing.T) {
	srv := NewTestServer(t, "127.0.0.1:9051")
	defer srv.Stop()

	db := NewSession(Config{
		Nodes:       []string{srv.Address},
		Consistency: ConQuorum,
	})

	if err := db.Query("slow").Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestRoundRobin(t *testing.T) {
	servers := make([]*TestServer, 5)
	addrs := make([]string, len(servers))
	for i := 0; i < len(servers); i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", 9051+i)
		servers[i] = NewTestServer(t, addrs[i])
		defer servers[i].Stop()
	}
	db := NewSession(Config{
		Nodes:       addrs,
		Consistency: ConQuorum,
	})
	time.Sleep(1 * time.Second)

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 5; j++ {
				if err := db.Query("void").Exec(); err != nil {
					t.Fatal(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	diff := 0
	for i := 1; i < len(servers); i++ {
		d := 0
		if servers[i].nreq > servers[i-1].nreq {
			d = int(servers[i].nreq - servers[i-1].nreq)
		} else {
			d = int(servers[i-1].nreq - servers[i].nreq)
		}
		if d > diff {
			diff = d
		}
	}

	if diff > 0 {
		t.Fatal("diff:", diff)
	}
}
