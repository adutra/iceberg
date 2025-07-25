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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.apache.iceberg.rest.oauth2.token.TypedToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTokenExchangeRequest {

  @ParameterizedTest
  @MethodSource
  void asFormParameters(TokenExchangeRequest input, Map<String, String> expected) {
    assertThat(input.asFormParameters()).containsExactlyInAnyOrderEntriesOf(expected);
  }

  public static Stream<Arguments> asFormParameters() {
    return Stream.of(
        Arguments.of(
            TokenExchangeRequest.builder()
                .subjectToken("subject-token1")
                .subjectTokenType(TypedToken.URN_ACCESS_TOKEN)
                .build(),
            Map.of(
                "grant_type", GrantType.TOKEN_EXCHANGE.canonicalName(),
                "subject_token", "subject-token1",
                "subject_token_type", TypedToken.URN_ACCESS_TOKEN.toString())),
        Arguments.of(
            TokenExchangeRequest.builder()
                .clientId("client1")
                .scope("scope1 scope2")
                .extraParameters(Map.of("extra1", "value1", "extra2", "value2"))
                .subjectToken("subject-token1")
                .subjectTokenType(TypedToken.URN_ACCESS_TOKEN)
                .actorToken("actor-token1")
                .actorTokenType(TypedToken.URN_ID_TOKEN)
                .requestedTokenType(TypedToken.URN_ACCESS_TOKEN)
                .resource(TestConstants.RESOURCE)
                .audience("audience1")
                .build(),
            ImmutableMap.builder()
                .put("grant_type", GrantType.TOKEN_EXCHANGE.canonicalName())
                .put("client_id", "client1")
                .put("scope", "scope1 scope2")
                .put("extra1", "value1")
                .put("extra2", "value2")
                .put("subject_token", "subject-token1")
                .put("subject_token_type", TypedToken.URN_ACCESS_TOKEN.toString())
                .put("actor_token", "actor-token1")
                .put("actor_token_type", TypedToken.URN_ID_TOKEN.toString())
                .put("requested_token_type", TypedToken.URN_ACCESS_TOKEN.toString())
                .put("resource", TestConstants.RESOURCE.toString())
                .put("audience", "audience1")
                .build()));
  }
}
