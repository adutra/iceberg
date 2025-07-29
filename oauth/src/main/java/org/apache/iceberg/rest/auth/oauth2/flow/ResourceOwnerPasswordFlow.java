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
import org.apache.iceberg.rest.auth.oauth2.config.Secret;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.PasswordTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.immutables.value.Value;

/**
 * An implementation of the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.3">Resource Owner Password
 * Credentials Grant</a> flow.
 *
 * <p>Note: according to the <a
 * href="https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics#section-2.4">OAuth
 * 2.0 Security Best Current Practice, section 2.4</a> this flow should NOT be used anymore because
 * it "insecurely exposes the credentials of the resource owner to the client".
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class ResourceOwnerPasswordFlow extends AbstractFlow implements InitialFlow {

  interface Builder extends AbstractFlow.Builder<ResourceOwnerPasswordFlow, Builder> {}

  @Override
  public GrantType grantType() {
    return GrantType.PASSWORD;
  }

  @Override
  public CompletionStage<Tokens> fetchNewTokens() {
    String username =
        spec()
            .resourceOwnerPasswordConfig()
            .username()
            .orElseThrow(() -> new IllegalStateException("Username is required"));
    String password =
        spec()
            .resourceOwnerPasswordConfig()
            .password()
            .map(Secret::value)
            .orElseThrow(() -> new IllegalStateException("Password is required"));
    PasswordTokenRequest.Builder request =
        PasswordTokenRequest.builder().username(username).password(password);
    return invokeTokenEndpoint(request, DefaultTokenResponse.class, null);
  }
}
