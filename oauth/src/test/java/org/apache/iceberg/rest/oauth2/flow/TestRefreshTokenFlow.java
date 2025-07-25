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
package org.apache.iceberg.rest.oauth2.flow;

import static org.apache.iceberg.rest.oauth2.test.TestConstants.ACCESS_TOKEN_EXPIRATION_TIME;
import static org.apache.iceberg.rest.oauth2.test.TestConstants.REFRESH_TOKEN_EXPIRATION_TIME;
import static org.apache.iceberg.rest.oauth2.test.TokenAssertions.assertTokens;

import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.oauth2.token.AccessToken;
import org.apache.iceberg.rest.oauth2.token.RefreshToken;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestRefreshTokenFlow {

  private final Tokens currentTokens =
      Tokens.of(
          AccessToken.of("access_initial", "Bearer", ACCESS_TOKEN_EXPIRATION_TIME),
          RefreshToken.of("refresh_initial", REFRESH_TOKEN_EXPIRATION_TIME));

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void fetchNewTokens(boolean privateClient, boolean returnRefreshTokens)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.AUTHORIZATION_CODE)
                .privateClient(privateClient)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        FlowFactory flowFactory = env.createFlowFactory()) {
      RefreshFlow flow = flowFactory.createTokenRefreshFlow();
      Tokens tokens = flow.refreshTokens(currentTokens).toCompletableFuture().get();
      assertTokens(
          tokens,
          "access_refreshed",
          returnRefreshTokens ? "refresh_refreshed" : "refresh_initial");
    }
  }
}
