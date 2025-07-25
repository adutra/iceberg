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
package org.apache.iceberg.rest.auth.oauth2.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import jakarta.annotation.Nullable;
import java.time.Instant;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;

public final class TokenAssertions {

  private static final Instant ACCESS_TOKEN_EXPIRATION_TIME =
      TestEnvironment.NOW.plusSeconds(TestEnvironment.ACCESS_TOKEN_EXPIRES_IN_SECONDS);
  private static final Instant REFRESH_TOKEN_EXPIRATION_TIME =
      TestEnvironment.NOW.plusSeconds(TestEnvironment.REFRESH_TOKEN_EXPIRES_IN_SECONDS);

  private TokenAssertions() {}

  public static void assertTokensResult(
      TokensResult result, String accessToken, @Nullable String refreshToken) {
    assertTokensResult(result, accessToken, refreshToken, refreshToken != null);
  }

  public static void assertTokensResult(
      TokensResult result,
      String accessToken,
      @Nullable String refreshToken,
      boolean expectRefreshTokenExp) {
    assertAccessToken(
        result.tokens().getAccessToken(),
        accessToken,
        TestEnvironment.ACCESS_TOKEN_EXPIRES_IN_SECONDS);
    assertRefreshToken(result.tokens().getRefreshToken(), refreshToken);
    assertThat(result.accessTokenExpirationTime()).isEqualTo(ACCESS_TOKEN_EXPIRATION_TIME);
    if (expectRefreshTokenExp) {
      assertThat(result.refreshTokenExpirationTime()).isEqualTo(REFRESH_TOKEN_EXPIRATION_TIME);
    } else {
      assertThat(result.refreshTokenExpirationTime()).isNull();
    }
  }

  public static void assertAccessToken(AccessToken actual, String expected, int expiresInSeconds) {
    assertThat(actual.getValue()).isEqualTo(expected);
    assertThat(actual.getLifetime()).isEqualTo(expiresInSeconds);
  }

  public static void assertRefreshToken(RefreshToken actual, String expected) {
    if (expected == null) {
      assertThat(actual).isNull();
    } else {
      assertThat(actual).isNotNull();
      assertThat(actual.getValue()).isEqualTo(expected);
    }
  }
}
