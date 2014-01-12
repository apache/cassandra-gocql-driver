// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tux21b.org/v1/gocql/uuid"
)

type TestServer struct {
	Address   string
	t         *testing.T
	nreq      int64
	listen    net.Listener
	hid       uuid.UUID
	dc        string
	rack      string
	bootstrap string
}

func TestSimple(t *testing.T) {
	srv := NewTestServer(t, "dc1", "rack1", true)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestClosed(t *testing.T) {
	srv := NewTestServer(t, "dc1", "rack1", true)
	defer srv.Stop()

	session, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	session.Close()

	if err := session.Query("void").Exec(); err != ErrUnavailable {
		t.Fatalf("expected %#v, got %#v", ErrUnavailable, err)
	}
}

func TestTimeout(t *testing.T) {
	srv := NewTestServer(t, "dc1", "rack1", true)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	go func() {
		<-time.After(1 * time.Second)
		t.Fatal("no timeout")
	}()

	if err := db.Query("kill").Exec(); err == nil {
		t.Fatal("expected error")
	}
}

func TestSlowQuery(t *testing.T) {
	srv := NewTestServer(t, "dc1", "rack1", true)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	if err := db.Query("slow").Exec(); err != nil {
		t.Fatal(err)
	}
	db.Close() //Close the server connections
}

func TestBootStrapping(t *testing.T) {
	srv := NewTestServer(t, "dc1", "rack1", false)
	if _, err := NewCluster(srv.Address).CreateSession(); err == nil {
		t.Fatal("Expected error as host is bootstrapping.")
	}

}

func TestRoundRobin(t *testing.T) {
	servers := make([]*TestServer, 5)
	addrs := make([]string, len(servers))
	for i := 0; i < len(servers); i++ {
		servers[i] = NewTestServer(t, "dc1", "rack1", true)
		addrs[i] = servers[i].Address
		defer servers[i].Stop()
	}
	cluster := NewCluster(addrs...)
	cluster.StartupMin = len(addrs)
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

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

func NewTestServer(t *testing.T, dc string, rack string, bootstrapped bool) *TestServer {
	strapped := "COMPLETED"
	if !bootstrapped {
		strapped = "BOOTSTRAPPING"
	}
	laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		t.Fatal(err)
	}
	srv := &TestServer{
		Address:   listen.Addr().String(),
		listen:    listen,
		t:         t,
		hid:       uuid.RandomUUID(),
		dc:        dc,
		rack:      rack,
		bootstrap: strapped,
	}
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
				atomic.AddInt64(&srv.nreq, 1)
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
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		switch strings.ToLower(first) {
		case "kill":
			select {}
		case "slow":
			go func() {
				<-time.After(1 * time.Second)
				frame.writeInt(resultKindVoid)
				frame.setLength(len(frame) - headerSize)
				if _, err := conn.Write(frame); err != nil {
					return
				}
			}()
			return
		case "use":
			frame.writeInt(resultKindKeyspace)
			frame.writeString(strings.TrimSpace(query[3:]))
		case "void":
			frame.writeInt(resultKindVoid)
		case "select":
			//Check to see if query is the local host lookup
			if query == "SELECT host_id,data_center,rack,bootstrapped FROM system.local" {
				frame.writeInt(resultKindRows)
				frame.writeInt(0x0001)      //flags set global table spec
				frame.writeInt(4)           //Number of columns
				frame.writeString("system") //Global Keyspace
				frame.writeString("local")  //Global Table name
				frame.writeString("host_id")
				frame.writeShort(0x000C) //UUID
				frame.writeString("data_center")
				frame.writeShort(0x000D) //String
				frame.writeString("rack")
				frame.writeShort(0x000D) //String
				frame.writeString("bootstrapped")
				frame.writeShort(0x000D) //String

				//Write row
				frame.writeInt(1)
				frame.writeBytes(srv.hid.Bytes())
				frame.writeBytes([]byte(srv.dc))
				frame.writeBytes([]byte(srv.rack))
				frame.writeBytes([]byte(srv.bootstrap))
			} else {
				frame.writeInt(resultKindVoid)
			}
			//To prevent failure in the round robin test
			atomic.AddInt64(&srv.nreq, -1)
		default:
			frame.writeInt(resultKindVoid)
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
