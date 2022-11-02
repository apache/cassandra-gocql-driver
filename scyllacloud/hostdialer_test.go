package scyllacloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

const (
	testTimeout = time.Second
)

func TestHostSNIDialer_InvalidConnectionConfig(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, serverCertPem, clientCertPem, clientKeyPem, err := setupTLSServer(nil)
	if err != nil {
		t.Fatal(err)
	}

	dialer := &gocql.ScyllaShardAwareDialer{Dialer: net.Dialer{}}

	tt := []struct {
		name          string
		connConfig    *ConnectionConfig
		hostInfo      *gocql.HostInfo
		expectedError error
	}{
		{
			name: "empty current context",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.CurrentContext = ""
				return cc
			}(),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't get current auth info: %w", fmt.Errorf("can't get current context config: %w", fmt.Errorf("current context can't be empty")))),
		},
		{
			name: "current context is unknown",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.CurrentContext = "unknown-context"
				return cc
			}(),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't get current auth info: %w", fmt.Errorf("can't get current context config: %w", fmt.Errorf(`context "unknown-context" does not exists`)))),
		},
		{
			name: "unknown default authInfo",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.Contexts[cc.CurrentContext].AuthInfoName = "unknown-authinfo"
				return cc
			}(),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't get current auth info: %w", fmt.Errorf(`authInfo "unknown-authinfo" does not exists`))),
		},
		{
			name:          "empty client certificate",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", serverCertPem, nil, clientKeyPem),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't read client certificate: %w", &os.PathError{Op: "open", Path: "", Err: syscall.ENOENT})),
		},
		{
			name:          "empty client key",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, nil),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't read client key: %w", &os.PathError{Op: "open", Path: "", Err: syscall.ENOENT})),
		},
		{
			name:          "empty certificate authority",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", nil, clientCertPem, clientKeyPem),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get root CA from configuration: %w", fmt.Errorf(`datacenter "us-east-1" does not include certificate authority`)),
		},
		{
			name: "unknown default datacenter",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.Contexts[cc.CurrentContext].DatacenterName = "unknown-datacenter"
				return cc
			}(),
			hostInfo:      &gocql.HostInfo{},
			expectedError: fmt.Errorf("can't get current datacenter config: %w", fmt.Errorf(`datacenter "unknown-datacenter" does not exists`)),
		},
		{
			name:       "unknown host datacenter",
			connConfig: newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem),
			hostInfo: func() *gocql.HostInfo {
				hi := &gocql.HostInfo{}
				hi.SetDatacenter("unknown-datacenter")
				hi.SetHostID("host-id")
				return hi
			}(),
			expectedError: fmt.Errorf(`datacenter "unknown-datacenter" configuration not found in connection bundle`),
		},
	}
	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hostDialer := NewSniHostDialer(tc.connConfig, dialer)
			_, err := hostDialer.DialHost(ctx, tc.hostInfo)
			if !reflect.DeepEqual(err, tc.expectedError) {
				t.Errorf("expected error to be %#v, got %#v", tc.expectedError, err)
			}
		})
	}
}

func TestHostSNIDialer_ServerNameIdentifiers(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name        string
		connConfig  func(server string, serverCertPem, clientCertPem, clientKeyPem []byte) *ConnectionConfig
		hostInfo    *gocql.HostInfo
		expectedSNI func(config *ConnectionConfig) string
	}{
		{
			name:       "node domain as SNI when host info is unknown",
			hostInfo:   &gocql.HostInfo{},
			connConfig: newBasicConnectionConf,
			expectedSNI: func(_ *ConnectionConfig) string {
				return "node.scylladb.com"
			},
		},
		{
			name:     "server as SNI when host info is unknown and node domain is empty",
			hostInfo: &gocql.HostInfo{},
			connConfig: func(server string, serverCertPem, clientCertPem, clientKeyPem []byte) *ConnectionConfig {
				cc := newBasicConnectionConf(server, serverCertPem, clientCertPem, clientKeyPem)
				dcConf := cc.Datacenters[cc.Contexts[cc.CurrentContext].DatacenterName]
				dcConf.NodeDomain = ""
				// Disable verification because serving cert isn't signed for IP address.
				dcConf.InsecureSkipTLSVerify = true
				return cc
			},
			expectedSNI: func(cc *ConnectionConfig) string {
				return cc.Datacenters[cc.Contexts[cc.CurrentContext].DatacenterName].Server
			},
		},
		{
			name: "host SNI when host is known",
			hostInfo: func() *gocql.HostInfo {
				hi := &gocql.HostInfo{}
				hi.SetHostID("host-1-uuid")
				hi.SetDatacenter("us-east-1")
				return hi
			}(),
			connConfig: newBasicConnectionConf,
			expectedSNI: func(_ *ConnectionConfig) string {
				return "host-1-uuid.node.scylladb.com"
			},
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			server, serverCertPem, clientCertPem, clientKeyPem, err := setupTLSServer([]string{"host-1-uuid.node.scylladb.com", "node.scylladb.com"})
			if err != nil {
				t.Fatal(err)
			}

			connectionStateCh := make(chan tls.ConnectionState, 1)
			server.TLS.VerifyConnection = func(state tls.ConnectionState) error {
				connectionStateCh <- state
				return nil
			}

			server.StartTLS()
			defer server.Close()

			dialer := &gocql.ScyllaShardAwareDialer{Dialer: net.Dialer{}}
			connConfig := tc.connConfig(server.Listener.Addr().String(), serverCertPem, clientCertPem, clientKeyPem)
			hostDialer := NewSniHostDialer(connConfig, dialer)

			_, err = hostDialer.DialHost(ctx, tc.hostInfo)
			if err != nil {
				t.Fatal(err)
			}

			select {
			case receivedState := <-connectionStateCh:
				expectedSNI := tc.expectedSNI(connConfig)
				if receivedState.ServerName != expectedSNI {
					t.Errorf("expected %q SNI, got %q", expectedSNI, receivedState.ServerName)
				}
			case <-ctx.Done():
				t.Fatal("expected to receive connection, but timed out")
			}
		})
	}
}

func setupTLSServer(dnsDomains []string) (*httptest.Server, []byte, []byte, []byte, error) {
	clientCert, clientKey, err := generateClientCert()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	clientCertPem, err := encodeCertificates(clientCert)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	clientKeyPem, err := encodePrivateKey(clientKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	clientCAPool := x509.NewCertPool()
	clientCAPool.AppendCertsFromPEM(clientCertPem)

	serverCert, serverKey, err := generateServingCert(dnsDomains)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	serverCertPem, err := encodeCertificates(serverCert)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	serverKeyPem, err := encodePrivateKey(serverKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	servingCert, err := tls.X509KeyPair(serverCertPem, serverKeyPem)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	server := httptest.NewUnstartedServer(nil)

	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{servingCert},
		ClientCAs:    clientCAPool,
		ClientAuth:   tls.RequestClientCert,
	}

	return server, serverCertPem, clientCertPem, clientKeyPem, nil
}

func newBasicConnectionConf(server string, serverCertPem, clientCertPem, clientKeyPem []byte) *ConnectionConfig {
	return &ConnectionConfig{
		Datacenters: map[string]*Datacenter{
			"us-east-1": {
				CertificateAuthorityData: serverCertPem,
				Server:                   server,
				NodeDomain:               "node.scylladb.com",
			},
		},
		AuthInfos: map[string]*AuthInfo{
			"admin": {
				ClientCertificateData: clientCertPem,
				ClientKeyData:         clientKeyPem,
			},
		},
		Contexts: map[string]*Context{
			"default": {
				DatacenterName: "us-east-1",
				AuthInfoName:   "admin",
			},
		},
		CurrentContext: "default",
	}
}

func generateServingCert(dnsNames []string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 1028)
	if err != nil {
		return nil, nil, fmt.Errorf("can't generate private key: %w", err)
	}

	commonName := "serving-cert"
	cert, err := generateSelfSignedX509Certificate(commonName, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, dnsNames, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}

func generateClientCert() (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 1028)
	if err != nil {
		return nil, nil, fmt.Errorf("can't generate private key: %w", err)
	}

	commonName := "client"
	cert, err := generateSelfSignedX509Certificate(commonName, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}

func generateSerialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	return serialNumber, nil
}

func generateSelfSignedX509Certificate(cn string, extKeyUsage []x509.ExtKeyUsage, dnsNames []string, pub, priv interface{}) (*x509.Certificate, error) {
	now := time.Now()

	serialNumber, err := generateSerialNumber()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: cn,
		},
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           extKeyUsage,
		NotBefore:             now.Add(-1 * time.Second),
		NotAfter:              now.Add(time.Hour),
		SignatureAlgorithm:    x509.SHA512WithRSA,
		BasicConstraintsValid: true,
		SerialNumber:          serialNumber,
		DNSNames:              dnsNames,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("can't create certificate: %w", err)
	}

	certs, err := x509.ParseCertificates(derBytes)
	if err != nil {
		return nil, fmt.Errorf("can't parse der encoded certificate: %w", err)
	}
	if len(certs) != 1 {
		return nil, fmt.Errorf("expected to parse 1 certificate from der bytes but %d were present", len(certs))
	}

	return certs[0], nil
}

func encodeCertificates(certificates ...*x509.Certificate) ([]byte, error) {
	buffer := bytes.Buffer{}
	for _, certificate := range certificates {
		err := pem.Encode(&buffer, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certificate.Raw,
		})
		if err != nil {
			return nil, fmt.Errorf("can't pem encode certificate: %w", err)
		}
	}
	return buffer.Bytes(), nil
}

func encodePrivateKey(key *rsa.PrivateKey) ([]byte, error) {
	buffer := bytes.Buffer{}
	err := pem.Encode(&buffer, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err != nil {
		return nil, fmt.Errorf("can't pem encode rsa private key: %w", err)
	}

	return buffer.Bytes(), nil
}
