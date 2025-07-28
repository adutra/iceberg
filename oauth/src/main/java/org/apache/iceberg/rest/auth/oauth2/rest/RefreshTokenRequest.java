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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-6">Token Request</a> that uses
 * the "refresh_tokens" grant type to refresh an existing access token.
 *
 * <p>Example:
 *
 * <pre>{@code
 * POST /token HTTP/1.1
 * Host: server.example.com
 * Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=refresh_token&refresh_token=tGzv3JOkF0XG5Qx2TlKWIA
 * }</pre>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class RefreshTokenRequest implements TokenRequest {

  public static final String REFRESH_TOKEN = "refresh_token";

  @Override
  public final GrantType grantType() {
    return GrantType.REFRESH_TOKEN;
  }

  /** The refresh token issued to the client. */
  public abstract String refreshToken();

  @Override
  public final Map<String, String> asFormParameters() {
    return ImmutableMap.<String, String>builder()
        .putAll(TokenRequest.super.asFormParameters())
        .put(REFRESH_TOKEN, refreshToken())
        .buildKeepingLast();
  }

  public static Builder builder() {
    return ImmutableRefreshTokenRequest.builder();
  }

  public interface Builder extends TokenRequest.Builder<RefreshTokenRequest, Builder> {

    @CanIgnoreReturnValue
    Builder refreshToken(String refreshToken);
  }
}
