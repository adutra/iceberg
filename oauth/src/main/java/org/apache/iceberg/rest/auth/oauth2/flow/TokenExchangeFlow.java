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
import java.util.concurrent.CompletionStage;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeResponse;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;
import org.immutables.value.Value;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class TokenExchangeFlow extends AbstractFlow implements InitialFlow {

  interface Builder extends AbstractFlow.Builder<TokenExchangeFlow, Builder> {
    @CanIgnoreReturnValue
    Builder subjectTokenStage(CompletionStage<TypedToken> subjectTokenStage);

    @CanIgnoreReturnValue
    Builder actorTokenStage(CompletionStage<TypedToken> actorTokenStage);
  }

  @Override
  public GrantType grantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  abstract CompletionStage<TypedToken> subjectTokenStage();

  abstract CompletionStage<TypedToken> actorTokenStage();

  @Override
  public CompletionStage<Tokens> fetchNewTokens() {
    return subjectTokenStage()
        .thenCombine(
            actorTokenStage(),
            (subjectToken, actorToken) -> {
              Preconditions.checkNotNull(subjectToken, "Invalid subjectToken: null");
              return TokenExchangeRequest.builder()
                  .subjectToken(subjectToken.payload())
                  .subjectTokenType(subjectToken.tokenType())
                  .actorToken(actorToken == null ? null : actorToken.payload())
                  .actorTokenType(actorToken == null ? null : actorToken.tokenType())
                  .resource(spec().tokenExchangeConfig().resource().orElse(null))
                  .audience(spec().tokenExchangeConfig().audience().orElse(null))
                  .requestedTokenType(spec().tokenExchangeConfig().requestedTokenType());
            })
        .thenCompose(request -> invokeTokenEndpoint(request, TokenExchangeResponse.class, null));
  }
}
