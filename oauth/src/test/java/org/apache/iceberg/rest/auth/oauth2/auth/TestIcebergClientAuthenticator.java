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
package org.apache.iceberg.rest.auth.oauth2.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.oauth2.config.Secret;
import org.apache.iceberg.rest.auth.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ClientRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.PasswordTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.apache.iceberg.rest.auth.oauth2.token.AccessToken;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.junit.jupiter.api.Test;

class TestIcebergClientAuthenticator {

  @Test
  void authenticateInitialTokenFetch() {
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
  void authenticateInitialTokenFetchNoClientId() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder()
            .clientSecret(Secret.of(TestConstants.CLIENT_SECRET1))
            .build();
    assertThat(authenticator.clientId()).isEmpty();
    assertThat(authenticator.clientSecret()).contains(Secret.of(TestConstants.CLIENT_SECRET1));
    ClientCredentialsTokenRequest.Builder builder = ClientCredentialsTokenRequest.builder();
    authenticator.authenticate(builder, Maps.newHashMap(), null);
    assertThat(builder.build())
        .extracting(
            ClientCredentialsTokenRequest::clientId, ClientCredentialsTokenRequest::clientSecret)
        .containsExactly(null, TestConstants.CLIENT_SECRET1);
  }

  @Test
  void authenticateInitialTokenFetchNoClientSecret() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder().clientId(TestConstants.CLIENT_ID1).build();
    assertThat(authenticator.clientId()).contains(TestConstants.CLIENT_ID1);
    assertThat(authenticator.clientSecret()).isEmpty();
    ClientCredentialsTokenRequest.Builder builder = ClientCredentialsTokenRequest.builder();
    assertThatThrownBy(() -> authenticator.authenticate(builder, Maps.newHashMap(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Iceberg REST dialect initial token fetches require client_secret to be set");
  }

  @Test
  void authenticateTokenRefreshWithBasicAuth() {
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
  void authenticateTokenRefreshWithBearerTokenAuth() {
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

  @Test
  void authenticateTokenRefreshWithMissingCredentials() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder().build();
    assertThat(authenticator.clientId()).isEmpty();
    assertThat(authenticator.clientSecret()).isEmpty();
    TokenExchangeRequest.Builder builder = TokenExchangeRequest.builder();
    Map<String, String> headers = Maps.newHashMap();
    assertThatThrownBy(() -> authenticator.authenticate(builder, headers, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Iceberg REST dialect token refreshes require either "
                + "client_id + client_secret or bearer token for authentication");
    assertThat(headers).doesNotContainKey("Authorization");
  }

  @Test
  void authenticateUnsupportedRequestType() {
    IcebergClientAuthenticator authenticator =
        ImmutableIcebergClientAuthenticator.builder().build();
    assertThat(authenticator.clientId()).isEmpty();
    assertThat(authenticator.clientSecret()).isEmpty();
    ClientRequest.Builder<?, ?> builder = PasswordTokenRequest.builder();
    Map<String, String> headers = Maps.newHashMap();
    try {
      authenticator.authenticate(builder, headers, null);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage())
          .isEqualTo(
              "Iceberg REST dialect does not support authentication for request type: "
                  + builder.getClass().getName());
    }
    assertThat(headers).doesNotContainKey("Authorization");
  }
}
