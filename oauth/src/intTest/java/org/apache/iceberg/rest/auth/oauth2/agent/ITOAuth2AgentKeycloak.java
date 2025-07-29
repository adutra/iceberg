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
package org.apache.iceberg.rest.auth.oauth2.agent;

import static org.apache.iceberg.rest.auth.oauth2.grant.GrantType.AUTHORIZATION_CODE;
import static org.apache.iceberg.rest.auth.oauth2.grant.GrantType.DEVICE_CODE;
import static org.apache.iceberg.rest.auth.oauth2.grant.GrantType.PASSWORD;
import static org.apache.iceberg.rest.auth.oauth2.grant.GrantType.TOKEN_EXCHANGE;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.auth.ClientAuthentication;
import org.apache.iceberg.rest.auth.oauth2.config.PkceTransformation;
import org.apache.iceberg.rest.auth.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.container.KeycloakTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.user.UserBehavior;
import org.apache.iceberg.rest.auth.oauth2.token.AccessToken;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

@ExtendWith(KeycloakTestEnvironment.class)
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2AgentKeycloak {

  @InjectSoftAssertions private SoftAssertions soft;

  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"CLIENT_CREDENTIALS", "PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void clientSecretBasic(GrantType initialGrantType, ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            envBuilder
                .grantType(initialGrantType)
                .clientAuthentication(ClientAuthentication.CLIENT_SECRET_BASIC)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      boolean expectRefreshToken = initialGrantType != GrantType.CLIENT_CREDENTIALS;
      assertAgent(agent, TestConstants.CLIENT_ID1, expectRefreshToken);
      // also test copy before and after close
      try (OAuth2Agent agent2 = agent.copy()) {
        assertAgent(agent2, TestConstants.CLIENT_ID1, expectRefreshToken);
      }

      agent.close();
      try (OAuth2Agent agent3 = agent.copy()) {
        assertAgent(agent3, TestConstants.CLIENT_ID1, expectRefreshToken);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"CLIENT_CREDENTIALS", "PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void clientSecretPost(GrantType initialGrantType, ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            envBuilder
                .grantType(initialGrantType)
                .clientAuthentication(ClientAuthentication.CLIENT_SECRET_POST)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      assertAgent(
          agent, TestConstants.CLIENT_ID1, initialGrantType != GrantType.CLIENT_CREDENTIALS);
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void publicClient(GrantType initialGrantType, ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            envBuilder
                .grantType(initialGrantType)
                .privateClient(false)
                .discoveryEnabled(false) // also test discovery disabled
                .clientId(TestConstants.CLIENT_ID2)
                .clientSecret(TestConstants.CLIENT_SECRET2)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      assertAgent(
          agent, TestConstants.CLIENT_ID2, initialGrantType != GrantType.CLIENT_CREDENTIALS);
    }
  }

  @ParameterizedTest
  @CsvSource({"false, S256", "true, S256", "true, PLAIN"})
  void pkce(
      boolean enabled,
      PkceTransformation transformation,
      ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            envBuilder
                .grantType(AUTHORIZATION_CODE)
                .pkceEnabled(enabled)
                .pkceTransformation(transformation)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      assertAgent(agent, TestConstants.CLIENT_ID1, true);
    }
  }

  /**
   * Tests a simple impersonation scenario with the agent using its own token as the subject token,
   * and no actor token. The agent swaps its token for another one, roughly equivalent. No refresh
   * tokens are present.
   */
  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"CLIENT_CREDENTIALS", "PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void impersonation1(GrantType subjectGrantType, ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(TypedToken.URN_ACCESS_TOKEN) // request only access token
                .subjectGrantType(subjectGrantType)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      assertAgent(agent, TestConstants.CLIENT_ID1, false);
    }
  }

  /**
   * Tests a simple impersonation scenario with the agent using its own token as the subject token,
   * and no actor token. The agent swaps its token for another one, roughly equivalent. Refresh
   * tokens are present, which is why the client credentials grant cannot be used for the subject
   * token.
   */
  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void impersonation2(GrantType subjectGrantType, ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(
                    TypedToken.URN_REFRESH_TOKEN) // request access and refresh tokens
                .subjectGrantType(subjectGrantType)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      assertAgent(
          agent, TestConstants.CLIENT_ID1, subjectGrantType != GrantType.CLIENT_CREDENTIALS);
    }
  }

  /**
   * Tests a simple delegation scenario with a fixed subject token obtained off-band, and the agent
   * using itws own access token as the actor token.
   */
  @Test
  void delegation1(ImmutableTestEnvironment.Builder envBuilder) {
    AccessToken subjectToken;
    try (TestEnvironment env = envBuilder.build();
        OAuth2Agent subjectAgent = env.createAgent()) {
      subjectToken = subjectAgent.authenticate();
    }

    try (TestEnvironment env =
            envBuilder.grantType(TOKEN_EXCHANGE).subjectToken(subjectToken.payload()).build();
        OAuth2Agent agent = env.createAgent()) {
      AccessToken accessToken = agent.authenticate();
      introspectToken(accessToken, TestConstants.CLIENT_ID1);
    }
  }

  /**
   * Tests a simple delegation scenario with a fixed actor token, obtained off-band, and the agent
   * using its own access token as the subject token.
   */
  @Test
  void delegation2(ImmutableTestEnvironment.Builder envBuilder) {
    AccessToken actorToken;
    try (TestEnvironment env = envBuilder.build();
        OAuth2Agent actorAgent = env.createAgent()) {
      actorToken = actorAgent.authenticate();
    }

    try (TestEnvironment env =
            envBuilder.grantType(TOKEN_EXCHANGE).actorToken(actorToken.payload()).build();
        OAuth2Agent agent = env.createAgent()) {
      AccessToken accessToken = agent.authenticate();
      introspectToken(accessToken, TestConstants.CLIENT_ID1);
    }
  }

  /**
   * Tests a delegation scenario where both the subject and actor tokens are dynamically obtained.
   * The subject token is obtained using the authorization code grant, and the actor token using the
   * client credentials grant. Refresh tokens are requested, except for the client credentials grant
   * where they are not supported.
   */
  @ParameterizedTest
  @EnumSource(
      value = GrantType.class,
      names = {"CLIENT_CREDENTIALS", "PASSWORD", "AUTHORIZATION_CODE", "DEVICE_CODE"})
  void delegation3(GrantType subjectGrantType, ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    boolean expectRefreshToken = subjectGrantType != GrantType.CLIENT_CREDENTIALS;
    try (TestEnvironment env =
            envBuilder
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(
                    expectRefreshToken ? TypedToken.URN_REFRESH_TOKEN : TypedToken.URN_ACCESS_TOKEN)
                .subjectTokenConfig(
                    Map.of(OAuth2Properties.Basic.GRANT_TYPE, subjectGrantType.canonicalName()))
                .actorTokenConfig(
                    Map.of(OAuth2Properties.Basic.GRANT_TYPE, GrantType.CLIENT_CREDENTIALS.name()))
                .build();
        OAuth2Agent agent = env.createAgent()) {
      assertAgent(agent, TestConstants.CLIENT_ID1, expectRefreshToken);
    }
  }

  /** Tests dynamically-obtained tokens with refresh forcibly disabled. */
  @Test
  void refreshDisabled(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env = envBuilder.grantType(PASSWORD).tokenRefreshEnabled(false).build();
        OAuth2Agent agent = env.createAgent()) {
      // initial grant
      Tokens firstTokens = agent.authenticateInternal();
      introspectToken(firstTokens.accessToken(), TestConstants.CLIENT_ID1);
      soft.assertThat(agent).extracting("tokenRefreshFuture").isNull();
    }
  }

  @Test
  void unauthorizedBadClientSecret(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env = envBuilder.clientSecret("BAD SECRET").build();
        OAuth2Agent agent = env.createAgent()) {
      soft.assertThatThrownBy(agent::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorResponse)
          .extracting(ErrorResponse::code)
          .isEqualTo(401);
    }
  }

  @Test
  void unauthorizedBadPassword(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env = envBuilder.grantType(PASSWORD).password("BAD PASSWORD").build();
        OAuth2Agent agent = env.createAgent()) {
      soft.assertThatThrownBy(agent::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorResponse)
          .extracting(ErrorResponse::code)
          .isEqualTo(401);
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
      try (OAuth2Agent agent = env.createAgent()) {
        soft.assertThatThrownBy(agent::authenticate)
            .asInstanceOf(type(OAuth2Exception.class))
            .extracting(OAuth2Exception::errorResponse)
            .extracting(ErrorResponse::code)
            .isEqualTo(400); // Keycloak replies with 400 instead of 401
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
      try (OAuth2Agent agent = env.createAgent()) {
        soft.assertThatThrownBy(agent::authenticate)
            .asInstanceOf(type(OAuth2Exception.class))
            .extracting(OAuth2Exception::errorResponse)
            .extracting(ErrorResponse::code, ErrorResponse::type)
            .containsExactly(400, "access_denied"); // Keycloak replies with 400 instead of 401
      }
    }
  }

  private void assertAgent(OAuth2Agent agent, String clientId, boolean expectRefreshToken)
      throws ExecutionException, InterruptedException {
    // initial grant
    Tokens initial = agent.authenticateInternal();
    introspectToken(initial.accessToken(), clientId);
    // token refresh
    if (expectRefreshToken) {
      soft.assertThat(initial.refreshToken()).isNotNull();
      Tokens refreshed = agent.refreshCurrentTokens(initial).toCompletableFuture().get();
      introspectToken(refreshed.accessToken(), clientId);
      soft.assertThat(refreshed.refreshToken()).isNotNull();
    } else {
      soft.assertThat(initial.refreshToken()).isNull();
    }
    // fetch new tokens
    Tokens renewed = agent.fetchNewTokens().toCompletableFuture().get();
    introspectToken(renewed.accessToken(), clientId);
    if (expectRefreshToken) {
      soft.assertThat(renewed.refreshToken()).isNotNull();
    } else {
      soft.assertThat(renewed.refreshToken()).isNull();
    }
  }

  private void introspectToken(AccessToken accessToken, String clientId) {
    soft.assertThat(accessToken).isNotNull();
    DecodedJWT jwt = JWT.decode(accessToken.payload());
    soft.assertThat(jwt).isNotNull();
    soft.assertThat(jwt.getClaim("azp").asString()).isEqualTo(clientId);
    soft.assertThat(jwt.getClaim("scope").asString()).contains(TestConstants.SCOPE1);
  }
}
