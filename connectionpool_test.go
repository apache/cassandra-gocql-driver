//go:build all || unit
// +build all unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"crypto/tls"
	"testing"
)

func TestSetupTLSConfig(t *testing.T) {
	tests := []struct {
		name                       string
		opts                       *SslOptions
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
