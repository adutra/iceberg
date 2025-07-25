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
package org.apache.iceberg.rest.auth.oauth2.config;

import static java.util.Collections.singletonList;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.ACTOR_TOKEN;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.ACTOR_TOKEN_TYPE;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.AUDIENCES;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.PREFIX;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.REQUESTED_TOKEN_TYPE;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.RESOURCE;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.SUBJECT_TOKEN;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.SUBJECT_TOKEN_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Config;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTokenExchangeConfig {

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testValidate(Map<String, String> properties, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> TokenExchangeConfig.fromProperties(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(PREFIX + SUBJECT_TOKEN_TYPE, "urn:ietf:params:oauth:token-type:id_token"),
            singletonList(
                "subject token type must be urn:ietf:params:oauth:token-type:access_token when using dynamic subject token (rest.auth.oauth2.token-exchange.subject-token-type)")),
        Arguments.of(
            Map.of(
                PREFIX + ACTOR_TOKEN_TYPE,
                "urn:ietf:params:oauth:token-type:id_token",
                PREFIX + ACTOR_TOKEN + BasicConfig.TOKEN_ENDPOINT,
                "https://actor-token-endpoint.com/token"),
            singletonList(
                "actor token type must be urn:ietf:params:oauth:token-type:access_token when using dynamic actor token (rest.auth.oauth2.token-exchange.actor-token-type)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(Map<String, String> properties, TokenExchangeConfig expected) {
    TokenExchangeConfig actual = TokenExchangeConfig.fromProperties(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testFromProperties() throws ParseException {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableTokenExchangeConfig.builder().build()),
        Arguments.of(
            Map.of(
                PREFIX + SUBJECT_TOKEN,
                "my-subject-token",
                PREFIX + SUBJECT_TOKEN_TYPE,
                "urn:ietf:params:oauth:token-type:jwt"),
            ImmutableTokenExchangeConfig.builder()
                .subjectToken(new TypelessAccessToken("my-subject-token"))
                .subjectTokenType(TokenTypeURI.parse("urn:ietf:params:oauth:token-type:jwt"))
                .build()),
        Arguments.of(
            Map.of(
                PREFIX + ACTOR_TOKEN,
                "my-actor-token",
                PREFIX + ACTOR_TOKEN_TYPE,
                "urn:ietf:params:oauth:token-type:jwt"),
            ImmutableTokenExchangeConfig.builder()
                .actorToken(new TypelessAccessToken("my-actor-token"))
                .actorTokenType(TokenTypeURI.parse("urn:ietf:params:oauth:token-type:jwt"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + REQUESTED_TOKEN_TYPE, "urn:ietf:params:oauth:token-type:jwt"),
            ImmutableTokenExchangeConfig.builder()
                .requestedTokenType(TokenTypeURI.parse("urn:ietf:params:oauth:token-type:jwt"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + RESOURCE, "https://example.com/api"),
            ImmutableTokenExchangeConfig.builder()
                .resource(URI.create("https://example.com/api"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + AUDIENCES, "https://example.com/resource"),
            ImmutableTokenExchangeConfig.builder()
                .addAudiences(new Audience("https://example.com/resource"))
                .build()),
        Arguments.of(
            Map.of(
                PREFIX + AUDIENCES, "https://example.com/resource1,https://example.com/resource2"),
            ImmutableTokenExchangeConfig.builder()
                .addAudiences(
                    new Audience("https://example.com/resource1"),
                    new Audience("https://example.com/resource2"))
                .build()),
        // test dynamic subject token config
        Arguments.of(
            Map.of(
                PREFIX + SUBJECT_TOKEN + "." + BasicConfig.ISSUER_URL,
                "https://subject-token-issuer.com",
                PREFIX + SUBJECT_TOKEN + "." + BasicConfig.GRANT_TYPE,
                "client_credentials",
                PREFIX + SUBJECT_TOKEN + "." + BasicConfig.CLIENT_ID,
                "subject-client-id",
                PREFIX + SUBJECT_TOKEN + "." + BasicConfig.CLIENT_SECRET,
                "subject-client-secret"),
            ImmutableTokenExchangeConfig.builder()
                .subjectTokenConfig(
                    ImmutableOAuth2Config.builder()
                        .basicConfig(
                            ImmutableBasicConfig.builder()
                                .issuerUrl(URI.create("https://subject-token-issuer.com"))
                                .grantType(GrantType.CLIENT_CREDENTIALS)
                                .clientId(new ClientID("subject-client-id"))
                                .clientSecret(new Secret("subject-client-secret"))
                                .build())
                        .build())
                .build()),
        // test dynamic actor token config
        Arguments.of(
            Map.of(
                PREFIX + ACTOR_TOKEN + "." + BasicConfig.ISSUER_URL,
                "https://actor-token-issuer.com",
                PREFIX + ACTOR_TOKEN + "." + BasicConfig.GRANT_TYPE,
                "client_credentials",
                PREFIX + ACTOR_TOKEN + "." + BasicConfig.CLIENT_ID,
                "actor-client-id",
                PREFIX + ACTOR_TOKEN + "." + BasicConfig.CLIENT_SECRET,
                "actor-client-secret"),
            ImmutableTokenExchangeConfig.builder()
                .actorTokenConfig(
                    ImmutableOAuth2Config.builder()
                        .basicConfig(
                            ImmutableBasicConfig.builder()
                                .issuerUrl(URI.create("https://actor-token-issuer.com"))
                                .grantType(GrantType.CLIENT_CREDENTIALS)
                                .clientId(new ClientID("actor-client-id"))
                                .clientSecret(new Secret("actor-client-secret"))
                                .build())
                        .build())
                .build()));
  }
}
