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

import static com.nimbusds.oauth2.sdk.GrantType.AUTHORIZATION_CODE;
import static com.nimbusds.oauth2.sdk.GrantType.CLIENT_CREDENTIALS;
import static com.nimbusds.oauth2.sdk.GrantType.DEVICE_CODE;
import static com.nimbusds.oauth2.sdk.GrantType.PASSWORD;
import static com.nimbusds.oauth2.sdk.GrantType.TOKEN_EXCHANGE;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_JWT;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_POST;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.NONE;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.PRIVATE_KEY_JWT;
import static com.nimbusds.oauth2.sdk.token.TokenTypeURI.ACCESS_TOKEN;
import static com.nimbusds.oauth2.sdk.token.TokenTypeURI.REFRESH_TOKEN;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID1;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID2;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID3;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID4;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID5;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_SECRET3;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.SCOPE1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.iceberg.rest.auth.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.apache.iceberg.rest.auth.oauth2.http.HttpClientType;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension;
import org.apache.iceberg.rest.auth.oauth2.test.user.UserBehavior;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

@ExtendWith(KeycloakExtension.class)
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2ClientKeycloak {

  private static boolean bouncyCastleAvailable;

  @InjectSoftAssertions private SoftAssertions soft;

  @BeforeAll
  static void probeForBouncyCastle() {
    try {
      Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");
      bouncyCastleAvailable = true;
    } catch (ClassNotFoundException ignored) {
      // ignored
    }
  }

  @CartesianTest
  void clientSecretBasic(
      @CartesianTest.Enum HttpClientType httpClientType,
      @EnumLike(excludes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType initialGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .httpClientType(httpClientType)
                .grantType(initialGrantType)
                .clientAuthenticationMethod(CLIENT_SECRET_BASIC)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, true);
    }
  }

  @CartesianTest
  void clientSecretPost(
      @CartesianTest.Enum HttpClientType httpClientType,
      @EnumLike(excludes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType initialGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .httpClientType(httpClientType)
                .grantType(initialGrantType)
                .clientAuthenticationMethod(CLIENT_SECRET_POST)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, true);
    }
  }

  @CartesianTest
  void publicClient(
      @CartesianTest.Enum HttpClientType httpClientType,
      @EnumLike(
              excludes = {
                "client_credentials",
                "refresh_token",
                "urn:ietf:params:oauth:grant-type:token-exchange"
              })
          GrantType initialGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .httpClientType(httpClientType)
                .grantType(initialGrantType)
                .clientAuthenticationMethod(NONE)
                .discoveryEnabled(false) // also test discovery disabled
                .clientId(new ClientID(CLIENT_ID2))
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID2, true);
    }
  }

  @CartesianTest
  void clientSecretJwt(
      @CartesianTest.Enum HttpClientType httpClientType,
      @EnumLike(excludes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType initialGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .httpClientType(httpClientType)
                .grantType(initialGrantType)
                .clientId(new ClientID(CLIENT_ID3))
                .clientSecret(new Secret(CLIENT_SECRET3))
                .clientAuthenticationMethod(CLIENT_SECRET_JWT)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID3, true);
    }
  }

  @CartesianTest
  void privateKeyJwt(
      @EnumLike(excludes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType initialGrantType,
      @Values(
              strings = {
                "/openssl/rsa_private_key_pkcs8.pem",
                "/openssl/rsa_private_key_pkcs1.pem",
                "/openssl/ecdsa_private_key.pem"
              })
          String resource,
      ImmutableTestEnvironment.Builder envBuilder,
      @TempDir Path tempDir)
      throws Exception {
    assumeThat(bouncyCastleAvailable || resource.contains("pkcs8"))
        .as("BouncyCastle is required for RSA PKCS#1 and ECDSA keys")
        .isTrue();
    Path privateKeyPath = copyPrivateKey(resource, tempDir);
    JWSAlgorithm algorithm = resource.contains("rsa") ? JWSAlgorithm.RS256 : JWSAlgorithm.ES256;
    String clientId = resource.contains("rsa") ? CLIENT_ID4 : CLIENT_ID5;
    try (TestEnvironment env =
            envBuilder
                .grantType(initialGrantType)
                .clientId(new ClientID(clientId))
                .clientAuthenticationMethod(PRIVATE_KEY_JWT)
                .jwsAlgorithm(algorithm)
                .privateKey(privateKeyPath)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, clientId, true);
    }
  }

  @CartesianTest
  void pkce(
      @Values(booleans = {true, false}) boolean enabled,
      @EnumLike CodeChallengeMethod method,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .grantType(AUTHORIZATION_CODE)
                .pkceEnabled(enabled)
                .codeChallengeMethod(method)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, true);
    }
  }

  @CartesianTest
  void httpsCallback(
      @EnumLike CodeChallengeMethod method,
      ImmutableTestEnvironment.Builder envBuilder,
      @TempDir Path tempDir)
      throws Exception {
    Path keyStorePath = tempDir.resolve("keystore.p12");
    try (InputStream is = getClass().getResourceAsStream("/openssl/keystore.p12")) {
      assertThat(is).isNotNull();
      Files.copy(is, keyStorePath);
    }

    try (TestEnvironment env =
            envBuilder
                .grantType(AUTHORIZATION_CODE)
                .codeChallengeMethod(method)
                .callbackHttps(true)
                .sslKeyStorePath(keyStorePath)
                .sslKeyStorePassword("s3cr3t")
                .sslKeyStoreAlias("1")
                .userSslContext(
                    SSLContextBuilder.create()
                        .loadTrustMaterial(keyStorePath.toFile(), "s3cr3t".toCharArray())
                        .build())
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, true);
    }
  }

  /**
   * Tests a simple impersonation scenario with the client using its own token as the subject token,
   * and no actor token. The client swaps its token for another one, roughly equivalent. No refresh
   * tokens are present.
   */
  @CartesianTest
  void impersonation1(
      @EnumLike(excludes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType subjectGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(ACCESS_TOKEN) // request only access token
                .subjectGrantType(subjectGrantType)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, false);
    }
  }

  /**
   * Tests a simple impersonation scenario with the client using its own token as the subject token,
   * and no actor token. The client swaps its token for another one, roughly equivalent. Refresh
   * tokens are present, which is why the client credentials grant cannot be used for the subject
   * token.
   */
  @CartesianTest
  void impersonation2(
      @EnumLike(
              excludes = {
                "client_credentials",
                "refresh_token",
                "urn:ietf:params:oauth:grant-type:token-exchange"
              })
          GrantType subjectGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(REFRESH_TOKEN) // request access and refresh tokens
                .subjectGrantType(subjectGrantType)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, true);
    }
  }

  /**
   * Tests a simple delegation scenario with a fixed subject token obtained off-band, and the client
   * using itws own access token as the actor token.
   */
  @Test
  void delegation1(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    AccessToken subjectToken;
    try (TestEnvironment env = envBuilder.build();
        OAuth2Client subjectClient = env.newClient()) {
      subjectToken = subjectClient.authenticate();
    }

    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .subjectToken(new TypelessAccessToken(subjectToken.getValue()))
                .build();
        OAuth2Client client = env.newClient()) {
      AccessToken accessToken = client.authenticate();
      introspectToken(accessToken, CLIENT_ID1);
    }
  }

  /**
   * Tests a simple delegation scenario with a fixed actor token, obtained off-band, and the client
   * using its own access token as the subject token.
   */
  @Test
  void delegation2(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    AccessToken actorToken;
    try (TestEnvironment env = envBuilder.build();
        OAuth2Client actorClient = env.newClient()) {
      actorToken = actorClient.authenticate();
    }

    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .actorToken(new TypelessAccessToken(actorToken.getValue()))
                .build();
        OAuth2Client client = env.newClient()) {
      AccessToken accessToken = client.authenticate();
      introspectToken(accessToken, CLIENT_ID1);
    }
  }

  /**
   * Tests a delegation scenario where both the subject and actor tokens are dynamically obtained.
   * The subject token is obtained using a variable code grant, and the actor token using the client
   * credentials grant. Refresh tokens are requested, except for the client credentials grant where
   * they are not supported.
   */
  @CartesianTest
  void delegation3(
      @EnumLike(excludes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType subjectGrantType,
      ImmutableTestEnvironment.Builder envBuilder)
      throws Exception {
    boolean requestRefreshToken = !subjectGrantType.equals(CLIENT_CREDENTIALS);
    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(requestRefreshToken ? REFRESH_TOKEN : ACCESS_TOKEN)
                .subjectGrantType(subjectGrantType)
                .actorGrantType(CLIENT_CREDENTIALS)
                .build();
        OAuth2Client client = env.newClient()) {
      assertClient(client, CLIENT_ID1, requestRefreshToken);
      // test copy before and after close to exercise copying of dependent clients
      try (OAuth2Client client2 = client.copy()) {
        assertClient(client2, CLIENT_ID1, requestRefreshToken);
      }

      client.close();
      try (OAuth2Client client3 = client.copy()) {
        assertClient(client3, CLIENT_ID1, requestRefreshToken);
      }
    }
  }

  /** Tests dynamically-obtained tokens with refresh forcibly disabled. */
  @Test
  void refreshDisabled(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env = envBuilder.grantType(PASSWORD).tokenRefreshEnabled(false).build();
        OAuth2Client client = env.newClient()) {
      // initial grant
      TokensResult firstTokens = client.authenticateInternal();
      introspectToken(firstTokens.tokens().getAccessToken(), CLIENT_ID1);
      soft.assertThat(client).extracting("tokenRefreshFuture").isNull();
    }
  }

  @Test
  void unauthorizedBadClientSecret(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env = envBuilder.clientSecret(new Secret("BAD SECRET")).build();
        OAuth2Client client = env.newClient()) {
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorObject)
          .extracting(ErrorObject::getHTTPStatusCode, ErrorObject::getCode)
          .containsExactly(401, "unauthorized_client");
    }
  }

  @Test
  void unauthorizedBadPassword(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env =
            envBuilder.grantType(PASSWORD).password(new Secret("BAD PASSWORD")).build();
        OAuth2Client client = env.newClient()) {
      soft.assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorObject)
          .extracting(ErrorObject::getHTTPStatusCode, ErrorObject::getCode)
          .containsExactly(401, "invalid_grant");
    }
  }

  @Test
  void unauthorizedBadCode(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env =
        envBuilder
            .grantType(AUTHORIZATION_CODE)
            .userBehavior(
                UserBehavior.builder()
                    .from(UserBehavior.INTEGRATION_TESTS)
                    .emulateFailure(true)
                    .build())
            .build()) {
      try (OAuth2Client client = env.newClient()) {
        soft.assertThatThrownBy(client::authenticate)
            .asInstanceOf(type(OAuth2Exception.class))
            .extracting(OAuth2Exception::errorObject)
            .extracting(ErrorObject::getHTTPStatusCode, ErrorObject::getCode)
            .containsExactly(400, "invalid_grant"); // Keycloak replies with 400 instead of 401
      }
    }
  }

  @Test
  void deviceCodeAccessDenied(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env =
        envBuilder
            .grantType(DEVICE_CODE)
            .userBehavior(
                UserBehavior.builder()
                    .from(UserBehavior.INTEGRATION_TESTS)
                    .emulateFailure(true)
                    .build())
            .build()) {
      try (OAuth2Client client = env.newClient()) {
        soft.assertThatThrownBy(client::authenticate)
            .asInstanceOf(type(OAuth2Exception.class))
            .extracting(OAuth2Exception::errorObject)
            .extracting(ErrorObject::getHTTPStatusCode, ErrorObject::getCode)
            .containsExactly(400, "access_denied"); // Keycloak replies with 400 instead of 401
      }
    }
  }

  @Test
  void clientCopy(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env = envBuilder.build();
        OAuth2Client client = env.newClient()) {
      try (OAuth2Client client2 = client.copy()) {
        assertClient(client2, CLIENT_ID1, false);
      }

      client.close();
      try (OAuth2Client client3 = client.copy()) {
        assertClient(client3, CLIENT_ID1, false);
      }
    }
  }

  private void assertClient(OAuth2Client client, String clientId, boolean refresh) throws Exception {
    // fetch initial tokens
    TokensResult initial = client.authenticateInternal();
    introspectToken(initial.tokens().getAccessToken(), clientId);
    // token refresh
    // Note: the client is configured to use token exchange when the initial grant is
    // client_credentials, and refresh_token otherwise. Keycloak is configured to support both.
    if (refresh) {
      TokensResult refreshed = client.refreshCurrentTokens(initial).toCompletableFuture().get();
      introspectToken(refreshed.tokens().getAccessToken(), clientId);
    }
    // fetch new tokens
    TokensResult renewed = client.fetchNewTokens().toCompletableFuture().get();
    introspectToken(renewed.tokens().getAccessToken(), clientId);
  }

  private void introspectToken(AccessToken accessToken, String clientId) throws ParseException {
    soft.assertThat(accessToken).isNotNull();
    JWT jwt = JWTParser.parse(accessToken.getValue());
    soft.assertThat(jwt).isNotNull();
    soft.assertThat(jwt.getJWTClaimsSet().getStringClaim("azp")).isEqualTo(clientId);
    soft.assertThat(jwt.getJWTClaimsSet().getStringClaim("scope")).contains(SCOPE1);
  }

  private Path copyPrivateKey(String resource, Path tempDir) throws IOException {
    try (InputStream src = getClass().getResourceAsStream(resource)) {
      assertThat(src).isNotNull();
      Path dest = tempDir.resolve("private-key.pem");
      Files.copy(src, dest);
      return dest;
    }
  }
}
