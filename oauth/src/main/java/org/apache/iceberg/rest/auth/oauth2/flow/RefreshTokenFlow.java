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

import java.util.concurrent.CompletionStage;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.RefreshTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.token.RefreshToken;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.immutables.value.Value;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-6">Token
 * Refresh</a> flow.
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class RefreshTokenFlow extends AbstractFlow implements RefreshFlow {

  interface Builder extends AbstractFlow.Builder<RefreshTokenFlow, Builder> {}

  @Override
  public CompletionStage<Tokens> refreshTokens(Tokens currentTokens) {
    Preconditions.checkNotNull(currentTokens, "Invalid currentTokens: null");
    RefreshToken refreshToken = currentTokens.refreshToken();
    Preconditions.checkNotNull(refreshToken, "Invalid refreshToken: null");
    RefreshTokenRequest.Builder request =
        RefreshTokenRequest.builder().refreshToken(refreshToken.payload());
    return invokeTokenEndpoint(request, DefaultTokenResponse.class)
        .thenApply(
            tokens -> {
              if (tokens.refreshToken() == null) {
                // If the server did not return a new refresh token,
                // it means we must keep the current one
                return Tokens.of(tokens.accessToken(), refreshToken);
              }

              return tokens;
            });
  }
}
