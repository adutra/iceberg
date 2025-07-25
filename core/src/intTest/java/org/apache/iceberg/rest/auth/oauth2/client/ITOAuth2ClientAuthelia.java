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
package org.apache.iceberg.rest.auth.oauth2.client;

import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_POST;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.AutheliaExtension.CLIENT_ID1;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.AutheliaExtension.CLIENT_ID2;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.AutheliaExtension.CLIENT_SECRET1;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.AutheliaExtension.CLIENT_SECRET2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import org.apache.iceberg.rest.auth.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.AutheliaExtension;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(AutheliaExtension.class)
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2ClientAuthelia {

  @InjectSoftAssertions private SoftAssertions soft;

  private static Path keyStorePath;

  @BeforeAll
  static void beforeAll(@TempDir Path tempDir) throws Exception {
    keyStorePath = tempDir.resolve("keystore.p12");
    try (InputStream is =
        ITOAuth2ClientAuthelia.class.getResourceAsStream("/openssl/keystore.p12")) {
      assertThat(is).isNotNull();
      Files.copy(is, keyStorePath);
    }
  }

  @Test
  void clientSecretBasic(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env =
            envBuilder
                .grantType(GrantType.CLIENT_CREDENTIALS)
                .clientAuthenticationMethod(CLIENT_SECRET_BASIC)
                .clientId(new ClientID(CLIENT_ID1))
                .clientSecret(new Secret(CLIENT_SECRET1))
                .sslTrustAll(false)
                .sslTrustStorePath(keyStorePath)
                .sslTrustStorePassword("s3cr3t")
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, env.authorizationServerUrl());
    }
  }

  @Test
  void clientSecretPost(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env =
            envBuilder
                .grantType(GrantType.CLIENT_CREDENTIALS)
                .clientAuthenticationMethod(CLIENT_SECRET_POST)
                .clientId(new ClientID(CLIENT_ID2))
                .clientSecret(new Secret(CLIENT_SECRET2))
                .sslTrustAll(false)
                .sslTrustStorePath(keyStorePath)
                .sslTrustStorePassword("s3cr3t")
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID2, env.authorizationServerUrl());
    }
  }

  @Test
  void unauthorizedBadClientSecret(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env =
            envBuilder
                .clientSecret(new Secret("BAD SECRET"))
                .sslTrustAll(false)
                .sslTrustStorePath(keyStorePath)
                .sslTrustStorePassword("s3cr3t")
                .build();
        OAuth2Client client = env.newClient()) {
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorObject)
          .extracting(ErrorObject::getHTTPStatusCode, ErrorObject::getCode)
          .containsExactly(401, "invalid_client");
    }
  }

  private void assertClient(OAuth2Client client, String clientId, URI issuer) throws Exception {
    // initial grant
    TokensResult initial = client.authenticateInternal();
    introspectToken(initial.tokens().getAccessToken(), clientId, issuer);
    soft.assertThat(initial.tokens().getRefreshToken()).isNull();
    // fetch new tokens
    TokensResult renewed = client.fetchNewTokens().toCompletableFuture().get();
    introspectToken(renewed.tokens().getAccessToken(), clientId, issuer);
    soft.assertThat(renewed.tokens().getRefreshToken()).isNull();
  }

  private void introspectToken(AccessToken accessToken, String clientId, URI issuer)
      throws ParseException {
    soft.assertThat(accessToken).isNotNull();
    JWT jwt = JWTParser.parse(accessToken.getValue());
    soft.assertThat(jwt).isNotNull();
    String actualIssuer = jwt.getJWTClaimsSet().getIssuer();
    String actualClientId = jwt.getJWTClaimsSet().getStringClaim("client_id");
    soft.assertThat(actualIssuer).isEqualTo(issuer.toString());
    soft.assertThat(actualClientId).isEqualTo(clientId);
  }
}
