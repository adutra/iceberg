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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableTokenExchangeRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableTokenExchangeResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenResponse;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
@SuppressWarnings("resource")
public abstract class TokenExchangeExpectation extends InitialTokenFetchExpectation {

  @Override
  protected PostFormRequest tokenRequestBody() {
    return ImmutableTokenExchangeRequest.builder()
        .clientId(
            testEnvironment().privateClient()
                ? null
                : String.format("(%s|%s)", TestConstants.CLIENT_ID1, TestConstants.CLIENT_ID2))
        .subjectToken(String.format("(%s|%s)", TestConstants.SUBJECT_TOKEN, "access_.*"))
        .subjectTokenType(TestConstants.SUBJECT_TOKEN_TYPE)
        .actorToken(String.format("(%s|%s)", TestConstants.ACTOR_TOKEN, "access_.*"))
        .actorTokenType(TestConstants.ACTOR_TOKEN_TYPE)
        .requestedTokenType(TestConstants.REQUESTED_TOKEN_TYPE)
        .audience(TestConstants.AUDIENCE)
        .resource(TestConstants.RESOURCE)
        .scope(String.format("(%s|%s)", TestConstants.SCOPE1, TestConstants.SCOPE2))
        .putExtraParameter("(extra1|extra2)", "(value1|value2)")
        .build();
  }

  @Override
  protected TokenResponse tokenResponseBody(String accessToken, String refreshToken) {
    TokenExchangeResponse.Builder builder =
        ImmutableTokenExchangeResponse.builder().issuedTokenType(TypedToken.URN_ACCESS_TOKEN);
    return buildTokenResponseBody(builder, accessToken, refreshToken).build();
  }
}
