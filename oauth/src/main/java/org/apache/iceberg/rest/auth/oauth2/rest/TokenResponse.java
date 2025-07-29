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
package org.apache.iceberg.rest.auth.oauth2.rest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Clock;
import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.token.AccessToken;
import org.apache.iceberg.rest.auth.oauth2.token.ImmutableAccessToken;
import org.apache.iceberg.rest.auth.oauth2.token.ImmutableRefreshToken;
import org.apache.iceberg.rest.auth.oauth2.token.RefreshToken;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Redacted;

/**
 * Common interface for successful responses in reply to a {@link TokenRequest}.
 *
 * <p>A token response is also a flattened representation of a {@link Tokens} pair; one can convert
 * a {@link TokenResponse} to a {@link Tokens} pair using the {@link #asTokens(Clock)} method.
 *
 * @see DefaultTokenResponse
 * @see TokenExchangeResponse
 */
public interface TokenResponse extends RESTResponse {

  /** Convert this response to a {@link Tokens} pair using the provided clock. */
  default Tokens asTokens(Clock clock) {

    Instant now = clock.instant();

    Integer accessExpiresIn = accessTokenExpiresInSeconds();
    AccessToken accessToken =
        ImmutableAccessToken.builder()
            .tokenType(tokenType())
            .payload(accessTokenPayload())
            .expirationTime(accessExpiresIn == null ? null : now.plusSeconds(accessExpiresIn))
            .build();

    String refreshTokenPayload = refreshTokenPayload();
    Integer refreshExpiresIn = refreshTokenExpiresInSeconds();
    RefreshToken refreshToken =
        refreshTokenPayload == null
            ? null
            : ImmutableRefreshToken.builder()
                .payload(refreshTokenPayload)
                .expirationTime(refreshExpiresIn == null ? null : now.plusSeconds(refreshExpiresIn))
                .build();

    return Tokens.of(accessToken, refreshToken);
  }

  /**
   * The type of the token issued as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-7.1">Section 7.1</a>. Value is
   * case-insensitive.
   *
   * <p>This is typically "Bearer".
   */
  String tokenType();

  /** The access token issued by the authorization server. */
  @Redacted
  @SuppressWarnings("SafeLoggingPropagation")
  String accessTokenPayload();

  /**
   * RECOMMENDED. The lifetime in seconds of the access token. For example, the value "3600" denotes
   * that the access token will expire in one hour from the time the response was generated. If
   * omitted, the authorization server SHOULD provide the expiration time via other means or
   * document the default value.
   */
  @Nullable
  Integer accessTokenExpiresInSeconds();

  /**
   * OPTIONAL. The refresh token, which can be used to obtain new access tokens using the same
   * authorization grant as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-6">Section 6</a>.
   *
   * <p>Note: in the client credentials flow (grant type {@link GrantType#CLIENT_CREDENTIALS}), as
   * per <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.3">Section 4.4.3</a>, "A
   * refresh token SHOULD NOT be included". Keycloak indeed does not include a refresh token in the
   * response to a client credentials flow, unless the client is configured with the attribute
   * "client_credentials.use_refresh_token" set to "true".
   */
  @Nullable
  @Redacted
  @SuppressWarnings("SafeLoggingPropagation")
  String refreshTokenPayload();

  /**
   * Not in the OAuth2 spec, but used by Keycloak. The lifetime in seconds of the refresh token,
   * when a refresh token is included in the response.
   */
  @Nullable
  Integer refreshTokenExpiresInSeconds();

  /**
   * OPTIONAL, if identical to the scope requested by the client; otherwise, REQUIRED. The scope of
   * the access token as described by <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">Section 3.3</a>.
   */
  @Nullable
  String scope();

  @Override
  @Check
  default void validate() {
    // Already validated by Immutables
  }

  interface Builder<T extends TokenResponse, B extends Builder<T, B>> {

    @CanIgnoreReturnValue
    B tokenType(String tokenType);

    @CanIgnoreReturnValue
    B accessTokenPayload(String accessTokenPayload);

    @CanIgnoreReturnValue
    B accessTokenExpiresInSeconds(@Nullable Integer accessTokenExpiresInSeconds);

    @CanIgnoreReturnValue
    B refreshTokenPayload(@Nullable String refreshTokenPayload);

    @CanIgnoreReturnValue
    B refreshTokenExpiresInSeconds(@Nullable Integer refreshTokenExpiresInSeconds);

    @CanIgnoreReturnValue
    B scope(@Nullable String scope);

    T build();
  }
}
