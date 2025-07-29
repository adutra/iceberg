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
package org.apache.iceberg.rest.oauth2.rest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.1.3">Token Request</a> using
 * the "authorization_code" grant type to obtain a new access token.
 *
 * <p>This class supports the <a href="https://datatracker.ietf.org/doc/html/rfc7636">PKCE</a>
 * extension to the OAuth 2.0 authorization code flow. The code verifier is only required if the
 * authorization server requires PKCE.
 *
 * <p>Example:
 *
 * <pre>{@code
 * POST /token HTTP/1.1
 * Host: server.example.com
 * Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=authorization_code&code=SplxlOBeZQQYbYS6WxSbIA
 * &redirect_uri=https%3A%2F%2Fclient%2Eexample%2Ecom%2Fcb
 * }</pre>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class AuthorizationCodeTokenRequest implements TokenRequest {

  public static final String CODE = "code";
  public static final String REDIRECT_URI = "redirect_uri";
  public static final String CODE_VERIFIER = "code_verifier";

  @Override
  public final GrantType grantType() {
    return GrantType.AUTHORIZATION_CODE;
  }

  /** The authorization code received from the authorization server. */
  public abstract String code();

  /** The redirect URI used in the initial request. */
  public abstract URI redirectUri();

  /**
   * The code verifier used in the initial request. This is only required if the authorization
   * server requires PKCE.
   *
   * @see <a href="https://www.rfc-editor.org/rfc/rfc7636#section-4.5">RFC 7636 Section 4.5</a>
   */
  @Nullable
  public abstract String codeVerifier();

  @Override
  public final Map<String, String> asFormParameters() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder
        .putAll(TokenRequest.super.asFormParameters())
        .put(CODE, code())
        .put(REDIRECT_URI, redirectUri().toString());

    String codeVerifier = codeVerifier();
    if (codeVerifier != null) {
      builder.put(CODE_VERIFIER, codeVerifier);
    }

    return builder.buildKeepingLast();
  }

  public static Builder builder() {
    return ImmutableAuthorizationCodeTokenRequest.builder();
  }

  public interface Builder extends TokenRequest.Builder<AuthorizationCodeTokenRequest, Builder> {
    @CanIgnoreReturnValue
    Builder code(String code);

    @CanIgnoreReturnValue
    Builder redirectUri(URI redirectUri);

    @CanIgnoreReturnValue
    Builder codeVerifier(@Nullable String codeVerifier);
  }
}
