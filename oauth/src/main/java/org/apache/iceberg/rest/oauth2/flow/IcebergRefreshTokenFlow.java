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

import java.util.concurrent.CompletionStage;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.rest.oauth2.token.AccessToken;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.apache.iceberg.rest.oauth2.token.TypedToken;
import org.immutables.value.Value;

/**
 * A specialized {@link TokenExchangeFlow} that is used to refresh access tokens, for the Iceberg
 * dialect only.
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class IcebergRefreshTokenFlow extends AbstractFlow implements RefreshFlow {

  interface Builder extends AbstractFlow.Builder<IcebergRefreshTokenFlow, Builder> {}

  @Override
  public GrantType grantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  @Override
  public CompletionStage<Tokens> refreshTokens(Tokens currentTokens) {
    Preconditions.checkNotNull(currentTokens, "Invalid currentTokens: null");
    AccessToken accessToken = currentTokens.accessToken();
    Preconditions.checkNotNull(accessToken, "Invalid accessToken: null");

    TypedToken subjectToken = TypedToken.of(accessToken);

    TokenExchangeRequest.Builder request =
        TokenExchangeRequest.builder()
            .subjectToken(subjectToken.payload())
            .subjectTokenType(subjectToken.tokenType())
            .resource(spec().tokenExchangeConfig().resource().orElse(null))
            .audience(spec().tokenExchangeConfig().audience().orElse(null))
            .requestedTokenType(spec().tokenExchangeConfig().requestedTokenType());

    return invokeTokenEndpoint(currentTokens, request);
  }

  @Override
  public boolean requiresRefreshToken() {
    return false;
  }
}
