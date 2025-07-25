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
package org.apache.iceberg.rest.auth.oauth2.flow;

import static org.apache.iceberg.rest.auth.oauth2.test.TokenAssertions.assertTokensResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class TestTokenExchangeFlow {

  @CartesianTest
  void fetchNewTokensStatic(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        FlowFactory flowFactory = env.newFlowFactory()) {
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      TokensResult tokens = flow.fetchNewTokens().toCompletableFuture().get();
      assertTokensResult(tokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }

  @CartesianTest
  void fetchNewTokensDynamic(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @EnumLike(
              includes = {
                "client_credentials",
                "authorization_code",
                "urn:ietf:params:oauth:grant-type:device_code"
              })
          GrantType grantType,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    assumeTrue(
        !grantType.equals(GrantType.CLIENT_CREDENTIALS)
            || !authenticationMethod.equals(ClientAuthenticationMethod.NONE));
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                // increase concurrency so that token fetches can happen in parallel
                .executorPoolSize(3)
                .returnRefreshTokens(returnRefreshTokens)
                // MockServer expectations require that we use the same
                // client authentication method for main and subject/actor tokens.
                .clientAuthenticationMethod(authenticationMethod)
                .subjectToken(Optional.empty())
                .subjectGrantType(grantType)
                .subjectClientAuthenticationMethod(authenticationMethod)
                .actorToken(Optional.empty())
                .actorGrantType(grantType)
                .actorClientAuthenticationMethod(authenticationMethod)
                .build();
        FlowFactory flowFactory = env.newFlowFactory()) {
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      TokensResult tokens = flow.fetchNewTokens().toCompletableFuture().get();
      assertTokensResult(tokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }
}
