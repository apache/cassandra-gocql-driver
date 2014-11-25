// +build all unit

package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinHostPort(t *testing.T) {
	tests := map[string]string{
		"127.0.0.1:0":                                 JoinHostPort("127.0.0.1", 0),
		"127.0.0.1:1":                                 JoinHostPort("127.0.0.1:1", 9142),
		"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:0": JoinHostPort("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 0),
		"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1": JoinHostPort("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1", 9142),
	}
	for k, v := range tests {
		if k != v {
			t.Fatalf("expected '%v', got '%v'", k, v)
		}
	}
}

type TestServer struct {
	Address  string
	t        *testing.T
	nreq     uint64
	listen   net.Listener
	nKillReq uint64
}

func TestSimple(t *testing.T) {
	srv := NewTestServer(t)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Error(err)
	}
}

func TestSSLSimple(t *testing.T) {
	srv := NewSSLTestServer(t)
	defer srv.Stop()

	db, err := createTestSslCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Error(err)
	}
}

func createTestSslCluster(hosts string) *ClusterConfig {
	cluster := NewCluster(hosts)
	cluster.SslOpts = &SslOptions{
		CertPath:               "testdata/pki/gocql.crt",
		KeyPath:                "testdata/pki/gocql.key",
		CaPath:                 "testdata/pki/ca.crt",
		EnableHostVerification: false,
	}
	return cluster
}

func TestClosed(t *testing.T) {
	t.Skip("Skipping the execution of TestClosed for now to try to concentrate on more important test failures on Travis")
	srv := NewTestServer(t)
	defer srv.Stop()

	session, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}
	session.Close()

	if err := session.Query("void").Exec(); err != ErrSessionClosed {
		t.Errorf("expected %#v, got %#v", ErrSessionClosed, err)
	}
}

func TestTimeout(t *testing.T) {
	srv := NewTestServer(t)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}

	go func() {
		<-time.After(2 * time.Second)
		t.Fatal("no timeout")
	}()

	if err := db.Query("kill").Exec(); err == nil {
		t.Fatal("expected error")
	}
}

// TestQueryRetry will test to make sure that gocql will execute
// the exact amount of retry queries designated by the user.
func TestQueryRetry(t *testing.T) {
	srv := NewTestServer(t)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}

	go func() {
		<-time.After(5 * time.Second)
		t.Fatal("no timeout")
	}()
	rt := &SimpleRetryPolicy{NumRetries: 1}

	qry := db.Query("kill").RetryPolicy(rt)
	if err := qry.Exec(); err == nil {
		t.Fatal("expected error")
	}
	requests := srv.nKillReq
	if requests != uint64(qry.Attempts()) {
		t.Fatalf("expected requests %v to match query attemps %v", requests, qry.Attempts())
	}
	//Minus 1 from the requests variable since there is the initial query attempt
	if requests-1 != uint64(rt.NumRetries) {
		t.Fatalf("failed to retry the query %v time(s). Query executed %v times", rt.NumRetries, requests-1)
	}
}

func TestSlowQuery(t *testing.T) {
	srv := NewTestServer(t)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}

	if err := db.Query("slow").Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestRoundRobin(t *testing.T) {
	servers := make([]*TestServer, 5)
	addrs := make([]string, len(servers))
	for i := 0; i < len(servers); i++ {
		servers[i] = NewTestServer(t)
		addrs[i] = servers[i].Address
		defer servers[i].Stop()
	}
	cluster := NewCluster(addrs...)
	db, err := cluster.CreateSession()
	time.Sleep(1 * time.Second) //Sleep to allow the Cluster.fillPool to complete

	if err != nil {
		t.Errorf("NewCluster: %v", err)
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

func TestConnClosing(t *testing.T) {
	t.Skip("Skipping until test can be ran reliably")
	srv := NewTestServer(t)
	defer srv.Stop()

	db, err := NewCluster(srv.Address).CreateSession()
	if err != nil {
		t.Errorf("NewCluster: %v", err)
	}
	defer db.Close()

	numConns := db.cfg.NumConns
	count := db.cfg.NumStreams * numConns

	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(wg *sync.WaitGroup) {
			wg.Done()
			db.Query("kill").Exec()
		}(wg)
	}

	wg.Wait()

	time.Sleep(1 * time.Second) //Sleep so the fillPool can complete.
	pool := db.Pool.(ConnectionPool)
	conns := pool.Size()

	if conns != numConns {
		t.Fatalf("Expected to have %d connections but have %d", numConns, conns)
	}
}

func NewTestServer(t *testing.T) *TestServer {
	laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		t.Fatal(err)
	}
	srv := &TestServer{Address: listen.Addr().String(), listen: listen, t: t}
	go srv.serve()
	return srv
}

func NewSSLTestServer(t *testing.T) *TestServer {
	pem, err := ioutil.ReadFile("testdata/pki/ca.crt")
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pem) {
		t.Errorf("Failed parsing or appending certs")
	}
	mycert, err := tls.LoadX509KeyPair("testdata/pki/cassandra.crt", "testdata/pki/cassandra.key")
	if err != nil {
		t.Errorf("could not load cert")
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{mycert},
		RootCAs:      certPool,
	}
	listen, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	srv := &TestServer{Address: listen.Addr().String(), listen: listen, t: t}
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
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		switch strings.ToLower(first) {
		case "kill":
			atomic.AddUint64(&srv.nKillReq, 1)
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
			frame.writeInt(3)
			frame.writeString(strings.TrimSpace(query[3:]))
		case "void":
			frame.writeInt(resultKindVoid)
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
