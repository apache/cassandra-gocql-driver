//go:build integration && scylla
// +build integration,scylla

package gocql_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/scyllacloud"
	"sigs.k8s.io/yaml"
)

func TestCloudConnection(t *testing.T) {
	if !*gocql.FlagRunSslTest {
		t.Skip("Skipping because SSL is not enabled on cluster")
	}

	const (
		sslPort        = 9142
		datacenterName = "datacenter1"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := map[string]string{}

	cluster := gocql.CreateCluster(func(config *gocql.ClusterConfig) {
		config.Port = sslPort
	})
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}

	var localAddress string
	var localHostID gocql.UUID
	scanner := session.Query("SELECT broadcast_address, host_id FROM system.local").Iter().Scanner()
	if scanner.Next() {
		if err := scanner.Scan(&localAddress, &localHostID); err != nil {
			t.Fatal(err)
		}
		hosts[localHostID.String()] = net.JoinHostPort(localAddress, fmt.Sprintf("%d", sslPort))
	}

	var peerAddress string
	var peerHostID gocql.UUID
	scanner = session.Query("SELECT peer, host_id FROM system.peers").Iter().Scanner()
	for scanner.Next() {
		if err := scanner.Scan(&peerAddress, &peerHostID); err != nil {
			t.Fatal(err)
		}
		hosts[peerHostID.String()] = net.JoinHostPort(peerAddress, fmt.Sprintf("%d", sslPort))
	}

	session.Close()

	logger := gocql.TestLogger
	defer func() {
		if t.Failed() {
			os.Stdout.WriteString(logger.String())
		}
	}()

	proxy := &sniProxy{
		hosts:          hosts,
		defaultBackend: net.JoinHostPort(localAddress, fmt.Sprintf("%d", sslPort)),
		logger:         logger,
	}

	proxyAddress, err := proxy.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	cc := &scyllacloud.ConnectionConfig{
		Datacenters: map[string]*scyllacloud.Datacenter{
			datacenterName: {
				CertificateAuthorityPath: "testdata/pki/ca.crt",
				Server:                   proxyAddress,
				TLSServerName:            "any",
				NodeDomain:               "cloud.scylladb.com",
				InsecureSkipTLSVerify:    true,
			},
		},
		AuthInfos: map[string]*scyllacloud.AuthInfo{
			"ai-1": {
				Username:              "username",
				Password:              "password",
				ClientKeyPath:         "testdata/pki/gocql.key",
				ClientCertificatePath: "testdata/pki/gocql.crt",
			},
		},
		Contexts: map[string]*scyllacloud.Context{
			"default-context": {
				AuthInfoName:   "ai-1",
				DatacenterName: datacenterName,
			},
		},
		CurrentContext: "default-context",
	}

	configPath, err := writeYamlToTempFile(cc)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(configPath)

	cluster, err = scyllacloud.NewCloudCluster(configPath)
	if err != nil {
		t.Fatal(err)
	}
	// Forward connections directed to node domain to our test sni proxy.
	cluster.Dialer = dialerContextFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.Contains(addr, cc.Datacenters[datacenterName].NodeDomain) {
			addr = cc.Datacenters[datacenterName].Server
		}
		return net.Dial(network, addr)
	})

	session, err = cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}

	if err := gocql.WaitUntilPoolsStopFilling(ctx, session, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	ringHosts := gocql.GetRingAllHosts(session)
	if len(ringHosts) != len(hosts) {
		t.Errorf("expected %d hosts in ring, got %d", len(hosts), len(ringHosts))
	}

	snisCount := map[string]int{}
	events := proxy.GetEvents()
	for _, event := range events {
		snisCount[event]++
	}

	for hostID := range hosts {
		sni := fmt.Sprintf("%s.%s", hostID, cc.Datacenters[datacenterName].NodeDomain)
		count, ok := snisCount[sni]
		if !ok {
			t.Errorf("not found connection to host %q", hostID)
		}
		if count != cluster.NumConns {
			t.Errorf("expected %d connections to host %q, got %d", cluster.NumConns, sni, count)
		}
	}
}

func writeYamlToTempFile(obj interface{}) (string, error) {
	f, err := os.CreateTemp(os.TempDir(), "gocql-cloud")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close temp file: %w", err)
	}

	buf, err := yaml.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("marshal yaml: %w", err)
	}
	if err := os.WriteFile(f.Name(), buf, 0600); err != nil {
		return "", fmt.Errorf("write to file %q: %w", f.Name(), err)
	}

	return f.Name(), nil
}

type dialerContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func (d dialerContextFunc) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d(ctx, network, addr)
}

type sniProxy struct {
	hosts          map[string]string
	defaultBackend string
	logger         gocql.StdLogger

	listener net.Listener
	events   []string
	mu       sync.Mutex
}

func (p *sniProxy) Run(ctx context.Context) (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("failed to listen: %w", err)
	}

	p.listener = listener

	go func() {
		for {
			conn, err := p.listener.Accept()
			if err != nil {
				p.logger.Println("failed to accept connection", err)
				return
			}

			go p.handleConnection(conn)
		}

	}()

	return listener.Addr().String(), nil
}

func (p *sniProxy) handleConnection(conn net.Conn) {
	defer conn.Close()

	var hello *tls.ClientHelloInfo

	peekedBytes := &bytes.Buffer{}
	// Ignore error because TLS library returns it when nil TLSConfig is returned.
	_ = tls.Server(readOnlyConn{reader: io.TeeReader(conn, peekedBytes)}, &tls.Config{
		GetConfigForClient: func(argHello *tls.ClientHelloInfo) (*tls.Config, error) {
			hello = &tls.ClientHelloInfo{}
			*hello = *argHello
			return nil, nil
		},
	}).Handshake()

	if hello == nil {
		p.logger.Println("client hello not sent")
		return
	}

	p.mu.Lock()
	p.events = append(p.events, hello.ServerName)
	p.mu.Unlock()

	backend, ok := p.hosts[hello.ServerName]
	if !ok {
		backend = p.defaultBackend
	}

	p.logger.Println("Dialing backend", backend, "SNI", hello.ServerName)
	backendConn, err := net.Dial("tcp", backend)
	if err != nil {
		p.logger.Println("failed to dial backend", backend, err)
		return
	}
	defer backendConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		_, _ = io.Copy(conn, backendConn)
		wg.Done()
	}()
	go func() {
		_, _ = io.Copy(backendConn, peekedBytes)
		_, _ = io.Copy(backendConn, conn)
		wg.Done()
	}()

	wg.Wait()
}

func (p *sniProxy) Close() error {
	return p.listener.Close()
}

func (p *sniProxy) GetEvents() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]string, 0, len(p.events))
	for _, e := range p.events {
		events = append(events, e)
	}
	return events
}

type readOnlyConn struct {
	reader io.Reader
}

var _ net.Conn = readOnlyConn{}

func (conn readOnlyConn) Read(p []byte) (int, error)         { return conn.reader.Read(p) }
func (conn readOnlyConn) Write(p []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (conn readOnlyConn) Close() error                       { return nil }
func (conn readOnlyConn) LocalAddr() net.Addr                { return nil }
func (conn readOnlyConn) RemoteAddr() net.Addr               { return nil }
func (conn readOnlyConn) SetDeadline(t time.Time) error      { return nil }
func (conn readOnlyConn) SetReadDeadline(t time.Time) error  { return nil }
func (conn readOnlyConn) SetWriteDeadline(t time.Time) error { return nil }
