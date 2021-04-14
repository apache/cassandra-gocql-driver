// +build all unit

package gocql

import (
	"crypto/tls"
	"testing"
)

func TestSetupTLSConfig(t *testing.T) {
	tests := []struct {
		name string
		opts *SslOptions
		expectedInsecureSkipVerify bool
	}{
		{
			name: "Config nil, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
			},
			expectedInsecureSkipVerify: true,
		},
		{
			name: "Config nil, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify false, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
				Config: &tls.Config{
					InsecureSkipVerify: false,
				},
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify true, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
				Config: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			expectedInsecureSkipVerify: true,
		},
		{
			name: "Config.InsecureSkipVerify false, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
				Config: &tls.Config{
					InsecureSkipVerify: false,
				},
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify true, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
				Config: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			expectedInsecureSkipVerify: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tlsConfig, err := setupTLSConfig(test.opts)
			if err != nil {
				t.Fatalf("unexpected error %q", err.Error())
			}
			if tlsConfig.InsecureSkipVerify != test.expectedInsecureSkipVerify {
				t.Fatalf("got %v, but expected %v", tlsConfig.InsecureSkipVerify,
					test.expectedInsecureSkipVerify)
			}
		})
	}
}

