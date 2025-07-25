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

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * The result of a successful token request, including the issued tokens and the time they were
 * issued.
 */
@Value.Immutable
public abstract class TokensResult {

  public static TokensResult of(AccessToken token) {
    return ImmutableTokensResult.builder().tokens(new Tokens(token, null)).build();
  }

  public static TokensResult of(AccessTokenResponse response, Instant issuedAt) {
    return of(response.toSuccessResponse().getTokens(), issuedAt, response.getCustomParameters());
  }

  public static TokensResult of(
      Tokens tokens, Instant issuedAt, Map<String, Object> customParameters) {
    return ImmutableTokensResult.builder()
        .tokens(tokens)
        .issuedAt(issuedAt)
        .customParameters(customParameters)
        .build();
  }

  /** The issued tokens. */
  public abstract Tokens tokens();

  /** The time the tokens were issued. */
  @Nullable
  public abstract Instant issuedAt();

  /** Custom parameters returned in the token response, if any. */
  abstract Map<String, Object> customParameters();

  /**
   * Returns true if the token is expired at the given time, inspecting the token's expiration time
   * and its JWT claims, if applicable. Note that if no expiration time is found, this method
   * returns false.
   */
  public boolean accessTokenExpired(Instant when) {
    Instant exp = accessTokenExpirationTime();
    return exp != null && !exp.isAfter(when);
  }

  /**
   * Returns true if the refresh token is expired at the given time, inspecting the token's
   * expiration time and its JWT claims, if applicable. Note that if no expiration time is found,
   * this method returns false.
   */
  public boolean refreshTokenExpired(Instant when) {
    Instant exp = refreshTokenExpirationTime();
    return exp != null && !exp.isAfter(when);
  }

  /**
   * The resolved expiration time of the access token, taking into account the response's {@code
   * expires_in} field and the JWT claims, if applicable.
   */
  @Value.Derived
  @Nullable
  public Instant accessTokenExpirationTime() {
    Instant exp = accessTokenResponseExpirationTime();
    return exp != null ? exp : accessTokenJwtExpirationTime();
  }

  @Value.Derived
  @Nullable
  public Instant refreshTokenExpirationTime() {
    Instant exp = refreshTokenResponseExpirationTime();
    return exp != null ? exp : refreshTokenJwtExpirationTime();
  }

  /** The access token expiration time as reported in the token response, if any. */
  @Value.Lazy
  @Nullable
  Instant accessTokenResponseExpirationTime() {
    Instant instant = issuedAt();
    return instant != null && tokens().getAccessToken().getLifetime() > 0
        ? instant.plusSeconds(tokens().getAccessToken().getLifetime())
        : null;
  }

  /**
   * The refresh token expiration time as reported in the token response, if any.
   *
   * <p>Note: The OAuth2 spec does not define a standard parameter for refresh token expiration, but
   * some providers use "refresh_expires_in" to indicate the lifetime of the refresh token in
   * seconds. This method checks for that parameter in the custom parameters map.
   */
  @Value.Lazy
  @Nullable
  @SuppressWarnings("EmptyCatch")
  Instant refreshTokenResponseExpirationTime() {
    Instant issuedAt = issuedAt();
    if (issuedAt != null && customParameters().containsKey("refresh_expires_in")) {
      try {
        long refreshExpiresIn = (long) customParameters().get("refresh_expires_in");
        if (refreshExpiresIn > 0) {
          return issuedAt.plusSeconds(refreshExpiresIn);
        }

        return null;
      } catch (Exception ignored) {
        // ignore
      }
    }

    return null;
  }

  /**
   * The access token JWT token expiration time, if the token is a JWT token and contains an
   * expiration claim.
   */
  @Value.Lazy
  @Nullable
  Instant accessTokenJwtExpirationTime() {
    try {
      return JWTParser.parse(tokens().getAccessToken().getValue())
          .getJWTClaimsSet()
          .getExpirationTime()
          .toInstant();
    } catch (Exception ignored) {
      return null;
    }
  }

  /**
   * The refresh token JWT token expiration time, if the token is a JWT token and contains an
   * expiration claim.
   */
  @Value.Lazy
  @Nullable
  Instant refreshTokenJwtExpirationTime() {
    try {
      return JWTParser.parse(tokens().getRefreshToken().getValue())
          .getJWTClaimsSet()
          .getExpirationTime()
          .toInstant();
    } catch (Exception ignored) {
      return null;
    }
  }
}
