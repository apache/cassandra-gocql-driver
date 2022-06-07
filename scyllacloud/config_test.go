// Copyright (C) 2021 ScyllaDB

package scyllacloud

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"sigs.k8s.io/yaml"
)

func TestCloudCluster(t *testing.T) {
	var (
		singleDCConfig = func() *ConnectionConfig {
			return &ConnectionConfig{
				Datacenters: map[string]*Datacenter{
					"dc-1": {
						CertificateAuthorityPath: "../testdata/pki/ca.crt",
						Server:                   "eu.cloud.scylladb.com",
						TLSServerName:            "some-host",
						NodeDomain:               "eu.cloud.scylladb.com",
					},
				},
				AuthInfos: map[string]*AuthInfo{
					"ai-1": {
						Username:              "username",
						Password:              "password",
						ClientKeyPath:         "../testdata/pki/gocql.key",
						ClientCertificatePath: "../testdata/pki/gocql.crt",
					},
				},
				Contexts: map[string]*Context{
					"default-context": {
						AuthInfoName:   "ai-1",
						DatacenterName: "dc-1",
					},
				},
				CurrentContext: "default-context",
			}
		}

		multiDCConfig = func() *ConnectionConfig {
			cc := singleDCConfig()
			cc.Datacenters["dc-2"] = &Datacenter{
				CertificateAuthorityPath: "../testdata/pki/ca.crt",
				Server:                   "cloud.scylladb.com",
				TLSServerName:            "some-host",
				NodeDomain:               "cloud.scylladb.com",
				ProxyURL:                 "socks5://127.0.0.1:5215",
			}
			return cc
		}
	)

	ts := []struct {
		name                string
		createConfig        func() (*ConnectionConfig, string)
		expectedError       error
		verifyClusterConfig func(*testing.T, *ConnectionConfig, *gocql.ClusterConfig)
	}{
		{
			name: "current context points to unknown context",
			createConfig: func() (*ConnectionConfig, string) {
				return writeCloudConnectionConfigToTemp(t, &ConnectionConfig{
					CurrentContext: "unknown",
				})
			},
			expectedError: fmt.Errorf("current context points to unknown context"),
		},
		{
			name: "context auth info points to unknown auth info",
			createConfig: func() (*ConnectionConfig, string) {
				return writeCloudConnectionConfigToTemp(t, &ConnectionConfig{
					Contexts: map[string]*Context{
						"default": {
							AuthInfoName: "unknown",
						},
					},
					CurrentContext: "default",
				})
			},
			expectedError: fmt.Errorf("context %q auth info points to unknown authinfo", "default"),
		},
		{
			name: "context datacenter points to unknown datacenter",
			createConfig: func() (*ConnectionConfig, string) {
				return writeCloudConnectionConfigToTemp(t, &ConnectionConfig{
					AuthInfos: map[string]*AuthInfo{
						"default": {},
					},
					Contexts: map[string]*Context{
						"default": {
							AuthInfoName:   "default",
							DatacenterName: "unknown",
						},
					},
					CurrentContext: "default",
				})
			},
			expectedError: fmt.Errorf("context %q datacenter points to unknown datacenter", "default"),
		},
		{
			name: "invalid default consistency",
			createConfig: func() (*ConnectionConfig, string) {
				cc := singleDCConfig()
				cc.Parameters = &Parameters{
					DefaultConsistency: DefaultSerialConsistency,
				}
				return writeCloudConnectionConfigToTemp(t, cc)
			},
			expectedError: fmt.Errorf("invalid value of default consistency %q, values can be one of: [THREE ONE TWO ANY QUORUM ALL LOCAL_QUORUM EACH_QUORUM LOCAL_ONE]", DefaultSerialConsistency),
		},
		{
			name: "invalid default serial consistency",
			createConfig: func() (*ConnectionConfig, string) {
				cc := singleDCConfig()
				cc.Parameters = &Parameters{
					DefaultSerialConsistency: DefaultQuorumConsistency,
				}
				return writeCloudConnectionConfigToTemp(t, cc)
			},
			expectedError: fmt.Errorf("invalid value of default serial consistency %q, values can be one of: [SERIAL LOCAL_SERIAL]", DefaultQuorumConsistency),
		},
		{
			name: "initial contact points are taken from all available datacenters",
			createConfig: func() (*ConnectionConfig, string) {
				return writeCloudConnectionConfigToTemp(t, multiDCConfig())
			},
			expectedError: nil,
			verifyClusterConfig: func(t *testing.T, connConfig *ConnectionConfig, config *gocql.ClusterConfig) {
				if len(connConfig.Datacenters) != len(config.Hosts) {
					t.Errorf("initial contact points does not use all datacenters")
				}
			},
		},
		{
			name: "certificate validation is off if any dc requires it",
			createConfig: func() (*ConnectionConfig, string) {
				cc := multiDCConfig()
				cc.Datacenters["dc-1"].InsecureSkipTLSVerify = true
				return writeCloudConnectionConfigToTemp(t, cc)
			},
			expectedError: nil,
			verifyClusterConfig: func(t *testing.T, connConfig *ConnectionConfig, config *gocql.ClusterConfig) {
				if !config.SslOpts.Config.InsecureSkipVerify {
					t.Errorf("expected disabled certificate verification")
				}
			},
		},
		{
			name: "authentication credentials are taken from current auth info",
			createConfig: func() (*ConnectionConfig, string) {
				return writeCloudConnectionConfigToTemp(t, singleDCConfig())
			},
			expectedError: nil,
			verifyClusterConfig: func(t *testing.T, connConfig *ConnectionConfig, config *gocql.ClusterConfig) {
				authenticator, ok := config.Authenticator.(gocql.PasswordAuthenticator)
				if !ok {
					t.Errorf("expected PasswordAuthenticator, got %T", config.Authenticator)
				}
				currentContext := connConfig.Contexts[connConfig.CurrentContext]
				authInfo := connConfig.AuthInfos[currentContext.AuthInfoName]
				if authInfo.Username != authenticator.Username {
					t.Errorf("expected %q username, got %q", authInfo.Username, authenticator.Username)
				}
				if authInfo.Password != authenticator.Password {
					t.Errorf("expected %q password, got %q", authInfo.Password, authenticator.Password)
				}
			},
		},
		{
			name: "certificate and key data has priority over path to file containing it",
			createConfig: func() (*ConnectionConfig, string) {
				cc := singleDCConfig()

				caCert, err := os.ReadFile(cc.Datacenters["dc-1"].CertificateAuthorityPath)
				if err != nil {
					t.Fatal(err)
				}
				cc.Datacenters["dc-1"].CertificateAuthorityData = caCert
				cc.Datacenters["dc-1"].CertificateAuthorityPath = "/not-existing-path"

				clientKey, err := os.ReadFile(cc.AuthInfos["ai-1"].ClientKeyPath)
				if err != nil {
					t.Fatal(err)
				}
				cc.AuthInfos["ai-1"].ClientKeyData = clientKey
				cc.AuthInfos["ai-1"].ClientKeyPath = "/not-existing-path"

				clientCert, err := os.ReadFile(cc.AuthInfos["ai-1"].ClientCertificatePath)
				if err != nil {
					t.Fatal(err)
				}
				cc.AuthInfos["ai-1"].ClientCertificateData = clientCert
				cc.AuthInfos["ai-1"].ClientCertificatePath = "/not-existing-path"

				return writeCloudConnectionConfigToTemp(t, cc)
			},
			expectedError: nil,
		},
	}

	for i := range ts {
		test := ts[i]
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cc, path := test.createConfig()
			defer os.RemoveAll(path)

			cloudConfig, err := NewCloudCluster(path)
			if !reflect.DeepEqual(test.expectedError, err) {
				t.Errorf("expected error %q, got %q", test.expectedError, err)
			}
			if test.verifyClusterConfig != nil {
				test.verifyClusterConfig(t, cc, cloudConfig)
			}
		})
	}
}

func writeCloudConnectionConfigToTemp(t *testing.T, cc *ConnectionConfig) (*ConnectionConfig, string) {
	f, err := os.CreateTemp(os.TempDir(), "gocql-cloud-conn-config-")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	buf, err := yaml.Marshal(cc)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(f.Name(), buf, 0600); err != nil {
		t.Fatal(err)
	}

	return cc, f.Name()
}
