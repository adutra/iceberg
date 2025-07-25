/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest.oauth2.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.oauth2.config.Secret;
import org.apache.iceberg.rest.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.apache.iceberg.rest.oauth2.token.AccessToken;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.junit.jupiter.api.Test;

class TestIcebergClientAuthenticator {

  @Test
  void authenticateClientSecretPost() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder()
            .clientId(TestConstants.CLIENT_ID1)
            .clientSecret(Secret.of(TestConstants.CLIENT_SECRET1))
            .build();
    assertThat(authenticator.clientId()).contains(TestConstants.CLIENT_ID1);
    assertThat(authenticator.clientSecret()).contains(Secret.of(TestConstants.CLIENT_SECRET1));
    ClientCredentialsTokenRequest.Builder builder = ClientCredentialsTokenRequest.builder();
    authenticator.authenticate(builder, Maps.newHashMap(), null);
    assertThat(builder.build())
        .extracting(
            ClientCredentialsTokenRequest::clientId, ClientCredentialsTokenRequest::clientSecret)
        .containsExactly(TestConstants.CLIENT_ID1, TestConstants.CLIENT_SECRET1);
  }

  @Test
  void authenticateClientSecretBasic() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder()
            .clientId(TestConstants.CLIENT_ID1)
            .clientSecret(Secret.of(TestConstants.CLIENT_SECRET1))
            .build();
    assertThat(authenticator.clientId()).contains(TestConstants.CLIENT_ID1);
    assertThat(authenticator.clientSecret()).contains(Secret.of(TestConstants.CLIENT_SECRET1));
    TokenExchangeRequest.Builder builder = TokenExchangeRequest.builder();
    Map<String, String> headers = Maps.newHashMap();
    authenticator.authenticate(builder, headers, null);
    assertThat(headers)
        .containsEntry("Authorization", "Basic " + TestConstants.CLIENT_CREDENTIALS1_BASE_64);
  }

  @Test
  void authenticateBearerToken() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder().build();
    assertThat(authenticator.clientId()).isEmpty();
    assertThat(authenticator.clientSecret()).isEmpty();
    TokenExchangeRequest.Builder builder = TokenExchangeRequest.builder();
    Tokens tokens = Tokens.of(AccessToken.of("token"), null);
    Map<String, String> headers = Maps.newHashMap();
    authenticator.authenticate(builder, headers, tokens);
    assertThat(headers).containsEntry("Authorization", "Bearer token");
  }
}
