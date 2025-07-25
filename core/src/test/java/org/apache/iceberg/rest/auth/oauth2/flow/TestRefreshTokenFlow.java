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
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class TestRefreshTokenFlow {

  private final Tokens currentTokens =
      new Tokens(new BearerAccessToken("access_initial"), new RefreshToken("refresh_initial"));

  @CartesianTest
  void fetchNewTokens(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @EnumLike(includes = {"refresh_token", "urn:ietf:params:oauth:grant-type:token-exchange"})
          GrantType grantType,
      @Values(booleans = {true, false}) boolean returnRefreshTokens,
      @Values(booleans = {true, false}) boolean returnRefreshTokenLifespan)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.AUTHORIZATION_CODE)
                .refreshGrantType(grantType)
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(returnRefreshTokens)
                .returnRefreshTokenLifespan(returnRefreshTokenLifespan)
                .build();
        FlowFactory flowFactory = env.newFlowFactory()) {
      assumeTrue(returnRefreshTokens || !returnRefreshTokenLifespan);
      Flow flow = flowFactory.newRefreshFlow(currentTokens);
      TokensResult tokens = flow.fetchNewTokens().toCompletableFuture().get();
      boolean refreshTokenGrant = grantType.equals(GrantType.REFRESH_TOKEN);
      assertThat(flow)
          .isInstanceOf(refreshTokenGrant ? RefreshTokenFlow.class : TokenExchangeFlow.class);
      assertTokensResult(
          tokens,
          "access_refreshed",
          returnRefreshTokens ? "refresh_refreshed" : refreshTokenGrant ? "refresh_initial" : null,
          returnRefreshTokenLifespan);
    }
  }
}
