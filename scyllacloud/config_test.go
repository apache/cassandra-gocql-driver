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
				t.Errorf("expected error %#v, got %#v", test.expectedError, err)
			}
			if test.verifyClusterConfig != nil {
				test.verifyClusterConfig(t, cc, cloudConfig)
			}
		})
	}
}

func TestConnectionConfig_GetCurrentContextConfig(t *testing.T) {
	tt := []struct {
		name            string
		connConfig      *ConnectionConfig
		expectedContext *Context
		expectedError   error
	}{
		{
			name: "empty current context",
			connConfig: &ConnectionConfig{
				CurrentContext: "",
			},
			expectedContext: nil,
			expectedError:   fmt.Errorf("current context can't be empty"),
		},
		{
			name: "not existing current context",
			connConfig: &ConnectionConfig{
				CurrentContext: "not-existing-context",
			},
			expectedContext: nil,
			expectedError:   fmt.Errorf(`context "not-existing-context" does not exists`),
		},
		{
			name: "context from current context is returned",
			connConfig: &ConnectionConfig{
				Contexts: map[string]*Context{
					"default": {
						AuthInfoName:   "admin",
						DatacenterName: "us-east-1",
					},
				},
				CurrentContext: "default",
			},
			expectedContext: &Context{
				AuthInfoName:   "admin",
				DatacenterName: "us-east-1",
			},
			expectedError: nil,
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			contextConf, err := tc.connConfig.GetCurrentContextConfig()
			if !reflect.DeepEqual(tc.expectedError, err) {
				t.Errorf("expected error %#v, got %#v", tc.expectedError, err)
			}
			if !reflect.DeepEqual(tc.expectedContext, contextConf) {
				t.Errorf("expected context %#v, got %#v", tc.expectedContext, contextConf)
			}
		})
	}
}

func TestConnectionConfig_GetCurrentAuthInfo(t *testing.T) {
	tt := []struct {
		name             string
		connConfig       *ConnectionConfig
		expectedAuthInfo *AuthInfo
		expectedError    error
	}{
		{
			name: "empty current context",
			connConfig: &ConnectionConfig{
				CurrentContext: "",
			},
			expectedAuthInfo: nil,
			expectedError:    fmt.Errorf("can't get current context config: %w", fmt.Errorf("current context can't be empty")),
		},
		{
			name: "not existing current context",
			connConfig: &ConnectionConfig{
				CurrentContext: "not-existing-context",
			},
			expectedAuthInfo: nil,
			expectedError:    fmt.Errorf("can't get current context config: %w", fmt.Errorf(`context "not-existing-context" does not exists`)),
		},
		{
			name: "empty auth info name in current context",
			connConfig: &ConnectionConfig{
				Contexts: map[string]*Context{
					"default": {
						AuthInfoName: "",
					},
				},
				CurrentContext: "default",
			},
			expectedAuthInfo: nil,
			expectedError:    fmt.Errorf("authInfo in current context can't be empty"),
		},
		{
			name: "not existing auth info name in current context",
			connConfig: &ConnectionConfig{
				Contexts: map[string]*Context{
					"default": {
						AuthInfoName: "not-existing-auth-info",
					},
				},
				CurrentContext: "default",
			},
			expectedAuthInfo: nil,
			expectedError:    fmt.Errorf(`authInfo "not-existing-auth-info" does not exists`),
		},
		{
			name: "auth info from current context is returned",
			connConfig: &ConnectionConfig{
				AuthInfos: map[string]*AuthInfo{
					"admin": {
						ClientCertificatePath: "client-cert-path",
						ClientKeyPath:         "client-key-path",
						Username:              "username",
						Password:              "password",
					},
				},
				Contexts: map[string]*Context{
					"default": {
						AuthInfoName: "admin",
					},
				},
				CurrentContext: "default",
			},
			expectedAuthInfo: &AuthInfo{
				ClientCertificatePath: "client-cert-path",
				ClientKeyPath:         "client-key-path",
				Username:              "username",
				Password:              "password",
			},
			expectedError: nil,
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			ai, err := tc.connConfig.GetCurrentAuthInfo()
			if !reflect.DeepEqual(tc.expectedError, err) {
				t.Errorf("expected error %#v, got %#v", tc.expectedError, err)
			}
			if !reflect.DeepEqual(tc.expectedAuthInfo, ai) {
				t.Errorf("expected authInfo %#v, got %#v", tc.expectedAuthInfo, ai)
			}
		})
	}
}

func TestConnectionConfig_GetCurrentDatacenterConfig(t *testing.T) {
	tt := []struct {
		name               string
		connConfig         *ConnectionConfig
		expectedDatacenter *Datacenter
		expectedError      error
	}{
		{
			name: "empty current context",
			connConfig: &ConnectionConfig{
				CurrentContext: "",
			},
			expectedDatacenter: nil,
			expectedError:      fmt.Errorf("can't get current context config: %w", fmt.Errorf("current context can't be empty")),
		},
		{
			name: "not existing current context",
			connConfig: &ConnectionConfig{
				CurrentContext: "not-existing-context",
			},
			expectedDatacenter: nil,
			expectedError:      fmt.Errorf("can't get current context config: %w", fmt.Errorf(`context "not-existing-context" does not exists`)),
		},
		{
			name: "empty datacenter name in current context",
			connConfig: &ConnectionConfig{
				Contexts: map[string]*Context{
					"default": {
						DatacenterName: "",
					},
				},
				CurrentContext: "default",
			},
			expectedDatacenter: nil,
			expectedError:      fmt.Errorf("datacenterName in current context can't be empty"),
		},
		{
			name: "not existing datacenter name in current context",
			connConfig: &ConnectionConfig{
				Contexts: map[string]*Context{
					"default": {
						DatacenterName: "not-existing-dc",
					},
				},
				CurrentContext: "default",
			},
			expectedDatacenter: nil,
			expectedError:      fmt.Errorf(`datacenter "not-existing-dc" does not exists`),
		},
		{
			name: "datacenter from current context is returned",
			connConfig: &ConnectionConfig{
				Datacenters: map[string]*Datacenter{
					"us-east-1": {
						CertificateAuthorityPath: "path-to-ca-cert",
						Server:                   "server",
						TLSServerName:            "tls-server-name",
						NodeDomain:               "node-domain",
						InsecureSkipTLSVerify:    true,
						ProxyURL:                 "proxy-url",
					},
				},
				Contexts: map[string]*Context{
					"default": {
						DatacenterName: "us-east-1",
					},
				},
				CurrentContext: "default",
			},
			expectedDatacenter: &Datacenter{
				CertificateAuthorityPath: "path-to-ca-cert",
				Server:                   "server",
				TLSServerName:            "tls-server-name",
				NodeDomain:               "node-domain",
				InsecureSkipTLSVerify:    true,
				ProxyURL:                 "proxy-url",
			},
			expectedError: nil,
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			dc, err := tc.connConfig.GetCurrentDatacenterConfig()
			if !reflect.DeepEqual(tc.expectedError, err) {
				t.Errorf("expected error %#v, got %#v", tc.expectedError, err)
			}
			if !reflect.DeepEqual(tc.expectedDatacenter, dc) {
				t.Errorf("expected datacenter %v, got %v", tc.expectedDatacenter, dc)
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
