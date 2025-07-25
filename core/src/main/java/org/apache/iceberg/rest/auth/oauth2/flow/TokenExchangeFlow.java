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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Token;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.immutables.value.Value;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 */
@Value.Immutable
abstract class TokenExchangeFlow extends FlowBase {

  interface Builder extends FlowBase.Builder<TokenExchangeFlow, Builder> {
    @CanIgnoreReturnValue
    Builder subjectTokenStage(CompletionStage<? extends Token> subjectTokenStage);

    @CanIgnoreReturnValue
    Builder actorTokenStage(CompletionStage<? extends Token> actorTokenStage);
  }

  @Override
  public final GrantType grantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  abstract CompletionStage<? extends Token> subjectTokenStage();

  abstract CompletionStage<? extends Token> actorTokenStage();

  @Override
  public CompletionStage<TokensResult> fetchNewTokens() {
    return subjectTokenStage()
        .thenCombine(
            actorTokenStage(),
            (subjectToken, actorToken) -> {
              Objects.requireNonNull(
                  subjectToken, "Cannot execute token exchange: missing required subject token");
              TokenExchangeConfig tokenExchangeConfig = config().tokenExchangeConfig();
              return newTokenExchangeGrant(subjectToken, actorToken, tokenExchangeConfig);
            })
        .thenCompose(this::invokeTokenEndpoint);
  }

  @Override
  TokenRequest.Builder newTokenRequestBuilder(AuthorizationGrant grant) {
    TokenRequest.Builder builder = super.newTokenRequestBuilder(grant);
    TokenExchangeConfig tokenExchangeConfig = config().tokenExchangeConfig();
    tokenExchangeConfig.resource().ifPresent(builder::resources);
    return builder;
  }

  private static TokenExchangeGrant newTokenExchangeGrant(
      Token subjectToken, Token actorToken, TokenExchangeConfig tokenExchangeConfig) {
    return new TokenExchangeGrant(
        subjectToken,
        tokenType(subjectToken),
        actorToken,
        actorToken == null ? null : tokenType(actorToken),
        tokenExchangeConfig.requestedTokenType(),
        tokenExchangeConfig.audiences());
  }

  private static TokenTypeURI tokenType(Token token) {
    if (token instanceof AccessToken && ((AccessToken) token).getIssuedTokenType() != null) {
      return ((AccessToken) token).getIssuedTokenType();
    }

    return TokenTypeURI.ACCESS_TOKEN;
  }
}
