// +build all unit

package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	defaultProto = protoVersion2
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

func TestSimple(t *testing.T) {
	srv := NewTestServer(t, defaultProto)
	defer srv.Stop()

	cluster := NewCluster(srv.Address)
	cluster.ProtoVersion = int(defaultProto)
	db, err := cluster.CreateSession()
	if err != nil {
		t.Errorf("0x%x: NewCluster: %v", defaultProto, err)
		return
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Errorf("0x%x: %v", defaultProto, err)
	}
}

func TestSSLSimple(t *testing.T) {
	srv := NewSSLTestServer(t, defaultProto)
	defer srv.Stop()

	db, err := createTestSslCluster(srv.Address, defaultProto).CreateSession()
	if err != nil {
		t.Fatalf("0x%x: NewCluster: %v", defaultProto, err)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("0x%x: %v", defaultProto, err)
	}
}

func createTestSslCluster(hosts string, proto uint8) *ClusterConfig {
	cluster := NewCluster(hosts)
	cluster.SslOpts = &SslOptions{
		CertPath:               "testdata/pki/gocql.crt",
		KeyPath:                "testdata/pki/gocql.key",
		CaPath:                 "testdata/pki/ca.crt",
		EnableHostVerification: false,
	}
	cluster.ProtoVersion = int(proto)
	return cluster
}

func TestClosed(t *testing.T) {
	t.Skip("Skipping the execution of TestClosed for now to try to concentrate on more important test failures on Travis")

	srv := NewTestServer(t, defaultProto)
	defer srv.Stop()

	cluster := NewCluster(srv.Address)
	cluster.ProtoVersion = int(defaultProto)

	session, err := cluster.CreateSession()
	defer session.Close()
	if err != nil {
		t.Errorf("0x%x: NewCluster: %v", defaultProto, err)
		return
	}

	if err := session.Query("void").Exec(); err != ErrSessionClosed {
		t.Errorf("0x%x: expected %#v, got %#v", defaultProto, ErrSessionClosed, err)
		return
	}
}

func newTestSession(addr string, proto uint8) (*Session, error) {
	cluster := NewCluster(addr)
	cluster.ProtoVersion = int(proto)
	return cluster.CreateSession()
}

func TestTimeout(t *testing.T) {

	srv := NewTestServer(t, defaultProto)
	defer srv.Stop()

	db, err := newTestSession(srv.Address, defaultProto)
	if err != nil {
		t.Errorf("NewCluster: %v", err)
		return
	}
	defer db.Close()

	go func() {
		<-time.After(2 * time.Second)
		t.Errorf("no timeout")
	}()

	if err := db.Query("kill").Exec(); err == nil {
		t.Errorf("expected error")
	}
}

// TestQueryRetry will test to make sure that gocql will execute
// the exact amount of retry queries designated by the user.
func TestQueryRetry(t *testing.T) {
	srv := NewTestServer(t, defaultProto)
	defer srv.Stop()

	db, err := newTestSession(srv.Address, defaultProto)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	go func() {
		<-time.After(5 * time.Second)
		t.Fatalf("no timeout")
	}()
	rt := &SimpleRetryPolicy{NumRetries: 1}

	qry := db.Query("kill").RetryPolicy(rt)
	if err := qry.Exec(); err == nil {
		t.Fatalf("expected error")
	}

	requests := atomic.LoadInt64(&srv.nKillReq)
	attempts := qry.Attempts()
	if requests != int64(attempts) {
		t.Fatalf("expected requests %v to match query attemps %v", requests, attempts)
	}

	//Minus 1 from the requests variable since there is the initial query attempt
	if requests-1 != int64(rt.NumRetries) {
		t.Fatalf("failed to retry the query %v time(s). Query executed %v times", rt.NumRetries, requests-1)
	}
}

func TestSlowQuery(t *testing.T) {
	srv := NewTestServer(t, defaultProto)
	defer srv.Stop()

	db, err := newTestSession(srv.Address, defaultProto)
	if err != nil {
		t.Errorf("NewCluster: %v", err)
		return
	}

	if err := db.Query("slow").Exec(); err != nil {
		t.Fatal(err)
	}
}

func TestRoundRobin(t *testing.T) {
	servers := make([]*TestServer, 5)
	addrs := make([]string, len(servers))
	for n := 0; n < len(servers); n++ {
		servers[n] = NewTestServer(t, defaultProto)
		addrs[n] = servers[n].Address
		defer servers[n].Stop()
	}
	cluster := NewCluster(addrs...)
	cluster.ProtoVersion = defaultProto

	db, err := cluster.CreateSession()
	time.Sleep(1 * time.Second) // Sleep to allow the Cluster.fillPool to complete

	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for n := 0; n < 5; n++ {
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
	for n := 1; n < len(servers); n++ {
		d := 0
		if servers[n].nreq > servers[n-1].nreq {
			d = int(servers[n].nreq - servers[n-1].nreq)
		} else {
			d = int(servers[n-1].nreq - servers[n].nreq)
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

	srv := NewTestServer(t, protoVersion2)
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

func TestStreams_Protocol1(t *testing.T) {
	srv := NewTestServer(t, protoVersion1)
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := NewCluster(srv.Address)
	cluster.NumConns = 1
	cluster.ProtoVersion = 1

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < db.cfg.NumStreams; i++ {
		// here were just validating that if we send NumStream request we get
		// a response for every stream and the lengths for the queries are set
		// correctly.
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Query("void").Exec(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestStreams_Protocol2(t *testing.T) {
	srv := NewTestServer(t, protoVersion2)
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := NewCluster(srv.Address)
	cluster.NumConns = 1
	cluster.ProtoVersion = 2

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < db.cfg.NumStreams; i++ {
		// the test server processes each conn synchronously
		// here were just validating that if we send NumStream request we get
		// a response for every stream and the lengths for the queries are set
		// correctly.
		if err = db.Query("void").Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStreams_Protocol3(t *testing.T) {
	srv := NewTestServer(t, protoVersion3)
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := NewCluster(srv.Address)
	cluster.NumConns = 1
	cluster.ProtoVersion = 3

	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < db.cfg.NumStreams; i++ {
		// the test server processes each conn synchronously
		// here were just validating that if we send NumStream request we get
		// a response for every stream and the lengths for the queries are set
		// correctly.
		if err = db.Query("void").Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkProtocolV3(b *testing.B) {
	srv := NewTestServer(b, protoVersion3)
	defer srv.Stop()

	// TODO: these are more like session tests and should instead operate
	// on a single Conn
	cluster := NewCluster(srv.Address)
	cluster.NumConns = 1
	cluster.ProtoVersion = 3

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err = db.Query("void").Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

func NewTestServer(t testing.TB, protocol uint8) *TestServer {
	laddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 8
	if protocol > protoVersion2 {
		headerSize = 9
	}

	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   protocol,
		headerSize: headerSize,
	}

	go srv.serve()

	return srv
}

func NewSSLTestServer(t testing.TB, protocol uint8) *TestServer {
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

	headerSize := 8
	if protocol > protoVersion2 {
		headerSize = 9
	}

	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   protocol,
		headerSize: headerSize,
	}
	go srv.serve()
	return srv
}

type TestServer struct {
	Address    string
	t          testing.TB
	nreq       uint64
	listen     net.Listener
	nKillReq   int64
	compressor Compressor

	protocol   byte
	headerSize int
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
				framer, err := srv.readFrame(conn)
				if err != nil {
					if err == io.EOF {
						return
					}

					srv.t.Error(err)
					return
				}

				atomic.AddUint64(&srv.nreq, 1)

				go srv.process(framer)
			}
		}(conn)
	}
}

func (srv *TestServer) Stop() {
	srv.listen.Close()
}

func (srv *TestServer) process(f *framer) {
	head := f.header
	if head == nil {
		srv.t.Error("process frame with a nil header")
		return
	}

	switch head.op {
	case opStartup:
		f.writeHeader(0, opReady, head.stream)
	case opOptions:
		f.writeHeader(0, opSupported, head.stream)
		f.writeShort(0)
	case opQuery:
		query := f.readLongString()
		first := query
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		switch strings.ToLower(first) {
		case "kill":
			atomic.AddInt64(&srv.nKillReq, 1)
			f.writeHeader(0, opError, head.stream)
			f.writeInt(0x1001)
			f.writeString("query killed")
		case "slow":
			go func() {
				<-time.After(1 * time.Second)
				f.writeHeader(0, opResult, head.stream)
				f.wbuf[0] = srv.protocol | 0x80
				f.writeInt(resultKindVoid)
				if err := f.finishWrite(); err != nil {
					srv.t.Error(err)
				}
			}()

			return
		case "use":
			f.writeInt(resultKindKeyspace)
			f.writeString(strings.TrimSpace(query[3:]))
		case "void":
			f.writeHeader(0, opResult, head.stream)
			f.writeInt(resultKindVoid)
		default:
			f.writeHeader(0, opResult, head.stream)
			f.writeInt(resultKindVoid)
		}
	default:
		f.writeHeader(0, opError, head.stream)
		f.writeInt(0)
		f.writeString("not supported")
	}

	f.wbuf[0] = srv.protocol | 0x80

	if err := f.finishWrite(); err != nil {
		srv.t.Error(err)
	}
}

func (srv *TestServer) readFrame(conn net.Conn) (*framer, error) {
	buf := make([]byte, srv.headerSize)
	head, err := readHeader(conn, buf)
	if err != nil {
		return nil, err
	}
	framer := newFramer(conn, conn, nil, srv.protocol)

	err = framer.readFrame(&head)
	if err != nil {
		return nil, err
	}

	// should be a request frame
	if head.version.response() {
		return nil, fmt.Errorf("expected to read a request frame got version: %v", head.version)
	} else if head.version.version() != srv.protocol {
		return nil, fmt.Errorf("expected to read protocol version 0x%x got 0x%x", srv.protocol, head.version.version())
	}

	return framer, nil
}
