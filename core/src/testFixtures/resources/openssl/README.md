<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# OpenSSL Generated Test Resources

## Overview

This directory contains test resources, mainly used for OAuth2 tests:

* `rsa_private_key_pkcs8.pem` - RSA private key in PKCS#8 format (`BEGIN PRIVATE KEY`)
* `rsa_certificate.pem` - Self-signed certificate from RSA key with CN="localhost" and
  SAN=DNS:localhost,IP:127.0.0.1
* `keystore.p12` - Java keystore containing `rsa_certificate.pem` and `rsa_private_key_pkcs8.pem`
  (password: `s3cr3t`)
* `mockserver.p12` - Mock Server's Java keystore containing its certificate and private key
  (password: `s3cr3t`)

> [!WARNING]
> These files are generated using `openssl` and are for testing purposes only. They are NOT
> secure and should NOT be used in production!

## Commands Summary

The files were generated using the following commands:

```shell
# 1. Generate RSA private key in PKCS#8 format
openssl genpkey -algorithm RSA -out rsa_private_key_pkcs8.pem

# 2. Generate long-lived self-signed certificate from RSA key (100 years)
openssl req -new -x509 -key rsa_private_key_pkcs8.pem -out rsa_certificate.pem -days 36500 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# 3. Generate Java keystore from RSA certificate and private key
openssl pkcs12 -export -in rsa_certificate.pem -inkey rsa_private_key_pkcs8.pem -out keystore.p12 -password pass:s3cr3t

# 4. Generate Java keystore from Mock Server's CA certificate and private key
wget https://raw.githubusercontent.com/mock-server/mockserver/refs/heads/master/mockserver-core/src/main/resources/org/mockserver/socket/CertificateAuthorityCertificate.pem -O mockserver.pem
wget https://raw.githubusercontent.com/mock-server/mockserver/refs/heads/master/mockserver-core/src/main/resources/org/mockserver/socket/CertificateAuthorityPrivateKey.pem -O mockserver.key
openssl pkcs12 -export -in mockserver.pem -inkey mockserver.key -out mockserver.p12 -password pass:s3cr3t
rm mockserver.pem mockserver.key
```
