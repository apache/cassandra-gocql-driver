#! /bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script generates the various certificates used for integration
# tests.  All certificates are created with a validity of 3650 days,
# or 10 years.  Therefore, this only needs to be used sparingly,
# although could eventually be repurposed to regenerate certificates
# as part of setting up the integration test harness. 

set -eux

# How long certificates should be considered valid, 100 years
VALIDITY=36500

# Generate 4096-bit unencrypted RSA private key using aes256
function generatePrivateKey() {
    base=$1
    rm -fv ${base}.key
    echo "Generating private key ${base}.key"
    # Generate Private Key
    openssl genrsa -aes256 -out ${base}.key -passout pass:cassandra 4096
    echo "Decrypting ${base}.key"
    # Decrypt Private Key
    openssl rsa -in ${base}.key -out ${base}.key -passin pass:cassandra
}

# Generate a X509 Certificate signed by the generated CA
function generateCASignedCert() {
    base=$1
    rm -fv ${base}.csr ${base}.crt
    # Generate Certificate Signing Request
    echo "Generating certificate signing request ${base}.csr"
    openssl req -new -key ${base}.key -out ${base}.csr -config ${base}.cnf
    # Generate Certificate using CA
    echo "Generating certificate ${base}.crt"
    openssl x509 -req -in ${base}.csr -CA ca.crt -CAkey ca.key \
                 -CAcreateserial -out ${base}.crt -days $VALIDITY \
                 -extensions req_ext -extfile ${base}.cnf -text
    rm -fv ${base}.csr
}

# CA
# Generate CA that signs both gocql and cassandra certs
generatePrivateKey ca
# Generate CA Certificate
echo "Generating CA certificate ca.crt"
rm -fv ca.crt
openssl req -x509 -new -nodes -key ca.key -days $VALIDITY \
            -out ca.crt -config ca.cnf -text

# Import CA certificate into JKS truststore so it can be used by Cassandra.
echo "Generating truststore .truststore for Cassandra"
rm -fv .truststore
keytool -import -keystore .truststore -trustcacerts \
        -file ca.crt -alias ca -storetype JKS \
        -storepass cassandra -noprompt

# GoCQL
# Generate CA-signed certificate for GoCQL client for integration tests
generatePrivateKey gocql
generateCASignedCert gocql

# Cassandra 
# Generate CA-signed certificate for Cassandra
generatePrivateKey cassandra
generateCASignedCert cassandra

# Import cassandra private key and certificate into a PKCS12 keystore
# and to a JKS keystore so it can be used by cassandra.
echo "Generating cassandra.p12 and .keystore for Cassandra"
rm -fv cassandra.p12
openssl pkcs12 -export -in cassandra.crt -inkey cassandra.key \
               -out cassandra.p12 -name cassandra \
               -CAfile ca.crt -caname ca \
               -password pass:cassandra \
               -noiter -nomaciter

rm -fv .keystore
keytool -importkeystore -srckeystore cassandra.p12 -srcstoretype PKCS12 \
	-srcstorepass cassandra -srcalias cassandra \
	-destkeystore .keystore -deststoretype JKS \
	-deststorepass cassandra -destalias cassandra
