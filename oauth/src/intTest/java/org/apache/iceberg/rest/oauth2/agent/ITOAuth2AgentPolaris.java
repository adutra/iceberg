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
package org.apache.iceberg.rest.oauth2.agent;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.oauth2.test.container.PolarisTestEnvironment;
import org.apache.iceberg.rest.oauth2.token.AccessToken;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for {@link OAuth2Agent} with Iceberg dialect against an Apache Polaris server. */
@ExtendWith(PolarisTestEnvironment.class)
@ExtendWith(SoftAssertionsExtension.class)
public class ITOAuth2AgentPolaris {

  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  void clientCredentials(ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env = envBuilder.build();
        OAuth2Agent agent = env.createAgent()) {
      // initial grant
      Tokens firstTokens = agent.authenticateInternal();
      introspectToken(firstTokens.accessToken());
      soft.assertThat(firstTokens.refreshToken()).isNull();
      // token refresh
      Tokens refreshedTokens = agent.refreshCurrentTokens(firstTokens).toCompletableFuture().get();
      introspectToken(refreshedTokens.accessToken());
      soft.assertThat(refreshedTokens.refreshToken()).isNull();
    }
  }

  /**
   * Tests a fixed initial token with Iceberg OAuth2 dialect. It's possible to refresh the token
   * with this dialect, however fetching a new token is not supported because there is no client id
   * and secret to authenticate with.
   */
  @Test
  void fixedToken(ImmutableTestEnvironment.Builder envBuilder)
      throws ExecutionException, InterruptedException {
    AccessToken accessToken;
    try (TestEnvironment env = envBuilder.build();
        OAuth2Agent agent = env.createAgent()) {
      accessToken = agent.authenticate();
    }

    try (TestEnvironment env = envBuilder.token(accessToken.payload()).build();
        OAuth2Agent agent = env.createAgent()) {
      // initial grant
      Tokens firstTokens = agent.authenticateInternal();
      soft.assertThat(firstTokens.accessToken().payload())
          .isEqualTo(accessToken.payload());
      soft.assertThat(firstTokens.refreshToken()).isNull();
      // token refresh
      Tokens refreshedTokens = agent.refreshCurrentTokens(firstTokens).toCompletableFuture().get();
      introspectToken(refreshedTokens.accessToken());
      soft.assertThat(refreshedTokens.refreshToken()).isNull();
      // cannot fetch new tokens
      soft.assertThat(agent.fetchNewTokens())
          .completesExceptionallyWithin(Duration.ofSeconds(10))
          .withThrowableOfType(ExecutionException.class)
          .withCauseInstanceOf(OAuth2Exception.class)
          .withMessageContaining("The Client is invalid");
    }
  }

  private void introspectToken(AccessToken accessToken) {
    DecodedJWT jwt = JWT.decode(accessToken.payload());
    soft.assertThat(jwt).isNotNull();
    soft.assertThat(jwt.getClaim("active").asBoolean()).isTrue();
    soft.assertThat(jwt.getClaim("scope").asString()).isEqualTo("PRINCIPAL_ROLE:ALL");
  }
}
