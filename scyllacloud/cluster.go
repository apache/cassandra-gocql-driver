// Copyright (C) 2021 ScyllaDB

package scyllacloud

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"github.com/gocql/gocql"
	"sigs.k8s.io/yaml"
)

func NewCloudCluster(bundlePath string) (*gocql.ClusterConfig, error) {
	connConf := &ConnectionConfig{}

	bundleFile, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("can't open bundle path: %w", err)
	}

	if err := yaml.Unmarshal(bundleFile, connConf); err != nil {
		return nil, fmt.Errorf("can't decode bundle file at %q: %w", bundlePath, err)
	}

	if _, ok := connConf.Contexts[connConf.CurrentContext]; !ok {
		return nil, fmt.Errorf("current context points to unknown context")
	}

	confContext := connConf.Contexts[connConf.CurrentContext]

	if _, ok := connConf.AuthInfos[confContext.AuthInfoName]; !ok {
		return nil, fmt.Errorf("context %q auth info points to unknown authinfo", connConf.CurrentContext)
	}

	if _, ok := connConf.Datacenters[confContext.DatacenterName]; !ok {
		return nil, fmt.Errorf("context %q datacenter points to unknown datacenter", connConf.CurrentContext)
	}

	authInfo := connConf.AuthInfos[confContext.AuthInfoName]

	caPool, err := connConf.GetRootCAPool()
	if err != nil {
		return nil, fmt.Errorf("can't create root CA pool: %w", err)
	}

	cc := gocql.NewCluster(connConf.GetInitialContactPoints()...)
	cc.Port = 443

	// SslOpts are used only by establishing connection to initial contact points.
	// Skip verifying TLS if any of DC requires it.
	insecureSkipVerify := false
	for _, dc := range connConf.Datacenters {
		if dc.InsecureSkipTLSVerify {
			insecureSkipVerify = true
			break
		}
	}

	cc.SslOpts = &gocql.SslOptions{
		// Set to false, always use value from tls.Config.
		EnableHostVerification: false,
		Config: &tls.Config{
			RootCAs: caPool,
			GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return connConf.GetClientCertificate()
			},
			InsecureSkipVerify: insecureSkipVerify,
		},
	}

	dialer := cc.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	cc.HostDialer = NewSniHostDialer(connConf, dialer)
	cc.Authenticator = gocql.PasswordAuthenticator{Password: authInfo.Password, Username: authInfo.Username}

	if connConf.Parameters != nil {
		if connConf.Parameters.DefaultConsistency != "" {
			if !validateConsistency(connConf.Parameters.DefaultConsistency, allowedConsistencies) {
				return nil, fmt.Errorf("invalid value of default consistency %q, values can be one of: %v", connConf.Parameters.DefaultConsistency, allowedConsistencies)
			}
			if err := cc.Consistency.UnmarshalText([]byte(connConf.Parameters.DefaultConsistency)); err != nil {
				return nil, fmt.Errorf("unmarshal default consistency: %w", err)
			}
		}
		if connConf.Parameters.DefaultSerialConsistency != "" {
			if !validateConsistency(connConf.Parameters.DefaultSerialConsistency, allowedSerialConsistencies) {
				return nil, fmt.Errorf("invalid value of default serial consistency %q, values can be one of: %v", connConf.Parameters.DefaultSerialConsistency, allowedSerialConsistencies)
			}
			if err := cc.SerialConsistency.UnmarshalText([]byte(connConf.Parameters.DefaultSerialConsistency)); err != nil {
				return nil, fmt.Errorf("unmarshal default serial consistency: %w", err)
			}
		}
	}

	return cc, nil
}

func validateConsistency(c ConsistencyString, allowed []ConsistencyString) bool {
	for _, ac := range allowed {
		if ac == c {
			return true
		}
	}
	return false
}

var allowedSerialConsistencies = []ConsistencyString{
	DefaultSerialConsistency,
	DefaultLocalSerialConsistency,
}
var allowedConsistencies = []ConsistencyString{
	DefaultThreeConsistency,
	DefaultOneConsistency,
	DefaultTwoConsistency,
	DefaultAnyConsistency,
	DefaultQuorumConsistency,
	DefaultAllConsistency,
	DefaultLocalQuorumConsistency,
	DefaultEachQuorumConsistency,
	DefaultLocalOneConsistency,
}
