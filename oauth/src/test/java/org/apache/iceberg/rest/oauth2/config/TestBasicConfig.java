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
package org.apache.iceberg.rest.oauth2.config;

import static java.util.Collections.singletonList;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.CLIENT_AUTH;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.CLIENT_ID;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.CLIENT_SECRET;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.DIALECT;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.EXTRA_PARAMS_PREFIX;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.GRANT_TYPE;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.ISSUER_URL;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.SCOPE;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.TIMEOUT;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.TOKEN;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.Basic.TOKEN_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.oauth2.auth.ClientAuthentication;
import org.apache.iceberg.rest.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestBasicConfig {

  @ParameterizedTest
  @MethodSource
  void testValidate(BasicConfig.Builder config, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(config::build)
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            BasicConfig.builder().clientId("Client1").clientSecret("s3cr3t"),
            singletonList(
                "either issuer URL or token endpoint must be set (rest.auth.oauth2.issuer-url / rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .issuerUrl(URI.create("realms/master")),
            singletonList("Issuer URL must not be relative (rest.auth.oauth2.issuer-url)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .issuerUrl(URI.create("https://example.com?query")),
            singletonList("Issuer URL must not have a query part (rest.auth.oauth2.issuer-url)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .issuerUrl(URI.create("https://example.com#fragment")),
            singletonList(
                "Issuer URL must not have a fragment part (rest.auth.oauth2.issuer-url)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com?query")),
            singletonList(
                "Token endpoint must not have a query part (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com#fragment")),
            singletonList(
                "Token endpoint must not have a fragment part (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token")),
            singletonList("client ID must not be empty (rest.auth.oauth2.client-id)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("client1")
                .clientAuthentication(ClientAuthentication.CLIENT_SECRET_BASIC)
                .tokenEndpoint(URI.create("https://example.com/token")),
            singletonList(
                "client secret must not be empty when client authentication is 'client_secret_basic' (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-secret)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("client1")
                .tokenEndpoint(URI.create("https://example.com/token")),
            singletonList(
                "client secret must not be empty when grant type is 'client_credentials' (rest.auth.oauth2.grant-type / rest.auth.oauth2.client-secret)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.REFRESH_TOKEN),
            singletonList(
                "grant type must be one of: 'client_credentials', 'password', 'authorization_code', 'token_exchange' (rest.auth.oauth2.grant-type)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("/tokens")),
            singletonList(
                "Token endpoint must not start with a slash when it's a relative URL (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            BasicConfig.builder()
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create(ResourcePaths.tokens()))
                .grantType(GrantType.PASSWORD),
            singletonList(
                "Iceberg OAuth2 dialect only supports the 'client_credentials' grant type (rest.auth.oauth2.grant-type / rest.auth.oauth2.dialect)")),
        Arguments.of(
            BasicConfig.builder().tokenEndpoint(URI.create(ResourcePaths.tokens())),
            singletonList(
                "client secret must not be empty when Iceberg OAuth2 dialect is used (rest.auth.oauth2.client-secret / rest.auth.oauth2.dialect)")),
        Arguments.of(
            BasicConfig.builder()
                .clientId("Client1")
                .clientSecret("s3cr3t")
                .issuerUrl(URI.create("https://example.com"))
                .timeout(Duration.ofSeconds(1)),
            singletonList(
                "timeout must be greater than or equal to PT30S (rest.auth.oauth2.timeout)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(
      Map<String, String> properties, BasicConfig expected, Throwable expectedThrowable) {
    if (expectedThrowable == null) {
      BasicConfig actual = BasicConfig.builder().from(properties).build();
      assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    } else {
      Throwable actual = catchThrowable(() -> BasicConfig.builder().from(properties));
      assertThat(actual)
          .isInstanceOf(expectedThrowable.getClass())
          .hasMessage(expectedThrowable.getMessage());
    }
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(null, null, new NullPointerException("Invalid properties map: null")),
        Arguments.of(
            ImmutableMap.builder()
                .put(ISSUER_URL, "https://example.com/")
                .put(TOKEN_ENDPOINT, "https://example.com/token")
                .put(GRANT_TYPE, "password")
                .put(CLIENT_ID, "Client")
                .put(CLIENT_SECRET, "w00t")
                .put(SCOPE, "test")
                .put(EXTRA_PARAMS_PREFIX + "extra1", "param1")
                .put(EXTRA_PARAMS_PREFIX + "extra2", "param 2")
                .put(EXTRA_PARAMS_PREFIX + "extra3", "") // empty
                .put(EXTRA_PARAMS_PREFIX, "") // malformed
                .put(TIMEOUT, "PT1M")
                .build(),
            BasicConfig.builder()
                .issuerUrl(URI.create("https://example.com/"))
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .clientId("Client")
                .clientSecret("w00t")
                .scopes(List.of("test"))
                .extraRequestParameters(ImmutableMap.of("extra1", "param1", "extra2", "param 2"))
                .timeout(Duration.ofMinutes(1))
                .build(),
            null),
        // Iceberg OAuth2 dialect
        Arguments.of(
            ImmutableMap.builder()
                .put(CLIENT_SECRET, "w00t")
                .put(TOKEN_ENDPOINT, ResourcePaths.tokens())
                .build(),
            BasicConfig.builder()
                .clientSecret("w00t")
                .tokenEndpoint(URI.create(ResourcePaths.tokens()))
                .dialect(Dialect.ICEBERG_REST)
                .build(),
            null),
        Arguments.of(
            ImmutableMap.builder()
                .put(DIALECT, "iceberg_rest")
                .put(CLIENT_SECRET, "w00t")
                .put(TOKEN_ENDPOINT, "https://example.com/token")
                .build(),
            BasicConfig.builder()
                .dialect(Dialect.ICEBERG_REST)
                .clientSecret("w00t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .build(),
            null),
        Arguments.of(
            ImmutableMap.builder().put(DIALECT, "iceberg_rest").put(CLIENT_SECRET, "w00t").build(),
            BasicConfig.builder()
                .dialect(Dialect.ICEBERG_REST)
                .clientSecret("w00t")
                // no token endpoint + iceberg dialect = internal token endpoint
                .tokenEndpoint(URI.create(ResourcePaths.tokens()))
                .build(),
            null),
        // Token
        Arguments.of(
            ImmutableMap.builder()
                .put(TOKEN, "token")
                .put(TOKEN_ENDPOINT, ResourcePaths.tokens())
                .build(),
            BasicConfig.builder()
                .token("token")
                .tokenEndpoint(URI.create(ResourcePaths.tokens()))
                .build(),
            null));
  }

  @ParameterizedTest
  @MethodSource
  void testMerge(BasicConfig base, Map<String, String> properties, BasicConfig expected) {
    BasicConfig merged = base.merge(properties);
    assertThat(merged).isEqualTo(expected);
  }

  static Stream<Arguments> testMerge() {
    return Stream.of(emptyProperties(), nonEmptyProperties(), nonEmptyProperties(), baseCleared());
  }

  private static Arguments emptyProperties() {
    BasicConfig base =
        BasicConfig.builder()
            .grantType(GrantType.CLIENT_CREDENTIALS)
            .dialect(Dialect.STANDARD)
            .clientId("Client1")
            .clientSecret("secret1")
            .issuerUrl(URI.create("https://example1.com"))
            .tokenEndpoint(URI.create("https://example1.com/token"))
            .scopes(List.of(TestConstants.SCOPE1))
            .extraRequestParameters(Map.of("extra1", "value1"))
            .build();
    return Arguments.of(base, Map.of(), base);
  }

  private static Arguments nonEmptyProperties() {
    BasicConfig base =
        BasicConfig.builder()
            .grantType(GrantType.PASSWORD)
            .dialect(Dialect.STANDARD)
            .clientId("Client1")
            .clientSecret("secret1")
            .issuerUrl(URI.create("https://example1.com"))
            .tokenEndpoint(URI.create("https://example1.com/token"))
            .scopes(List.of(TestConstants.SCOPE1))
            .extraRequestParameters(Map.of("extra1", "value1", "extra2", "value2"))
            .build();
    Map<String, String> properties =
        Map.of(
            GRANT_TYPE,
            GrantType.CLIENT_CREDENTIALS.name(),
            DIALECT,
            Dialect.ICEBERG_REST.name(),
            CLIENT_ID,
            "Client2",
            CLIENT_SECRET,
            "secret2",
            ISSUER_URL,
            "https://example2.com",
            TOKEN_ENDPOINT,
            "https://example2.com/token",
            SCOPE,
            TestConstants.SCOPE2,
            EXTRA_PARAMS_PREFIX + "extra2",
            "value2",
            EXTRA_PARAMS_PREFIX + "extra3",
            "value3");
    BasicConfig expected =
        BasicConfig.builder()
            .grantType(GrantType.CLIENT_CREDENTIALS)
            .dialect(Dialect.ICEBERG_REST)
            .clientId("Client2")
            .clientSecret("secret2")
            .issuerUrl(URI.create("https://example2.com"))
            .tokenEndpoint(URI.create("https://example2.com/token"))
            .scopes(List.of(TestConstants.SCOPE2))
            .extraRequestParameters(
                Map.of("extra1", "value1", "extra2", "value2", "extra3", "value3"))
            .build();
    return Arguments.of(base, properties, expected);
  }

  private static Arguments baseCleared() {
    BasicConfig base =
        BasicConfig.builder()
            .grantType(GrantType.PASSWORD)
            .dialect(Dialect.STANDARD)
            .token("token")
            .clientId("Client1")
            .clientSecret("secret1")
            .issuerUrl(URI.create("https://example1.com"))
            .tokenEndpoint(URI.create("https://example1.com/token"))
            .scopes(List.of(TestConstants.SCOPE1))
            .extraRequestParameters(Map.of("extra1", "value1", "extra2", "value2"))
            .build();
    Map<String, String> properties =
        Map.of(
            TOKEN,
            "",
            CLIENT_AUTH,
            "none",
            CLIENT_SECRET,
            "",
            ISSUER_URL,
            "",
            SCOPE,
            "",
            EXTRA_PARAMS_PREFIX + "extra2",
            "",
            EXTRA_PARAMS_PREFIX + "extra3",
            "");
    BasicConfig expected =
        BasicConfig.builder()
            .grantType(GrantType.PASSWORD)
            .dialect(Dialect.STANDARD)
            .clientId("Client1")
            .tokenEndpoint(URI.create("https://example1.com/token"))
            .extraRequestParameters(Map.of("extra1", "value1"))
            .build();
    return Arguments.of(base, properties, expected);
  }

  @ParameterizedTest
  @MethodSource
  void testDialect(Map<String, String> properties, Dialect expected) {
    BasicConfig config = BasicConfig.builder().from(properties).build();
    assertThat(config.dialect()).isEqualTo(expected);
  }

  static Stream<Arguments> testDialect() {
    return Stream.of(
        // Explicit
        Arguments.of(
            Map.of(
                ISSUER_URL,
                "https://example.com",
                CLIENT_ID,
                "Client",
                CLIENT_SECRET,
                "secret",
                DIALECT,
                Dialect.STANDARD.name()),
            Dialect.STANDARD),
        Arguments.of(
            Map.of(
                ISSUER_URL,
                "https://example.com",
                CLIENT_SECRET,
                "secret",
                DIALECT,
                Dialect.ICEBERG_REST.name()),
            Dialect.ICEBERG_REST),
        // Implicit
        Arguments.of(
            Map.of(ISSUER_URL, "https://example.com", CLIENT_ID, "Client", CLIENT_SECRET, "secret"),
            Dialect.STANDARD),
        // a token is present => Iceberg dialect
        Arguments.of(
            Map.of(ISSUER_URL, "https://example.com", TOKEN, "token"), Dialect.ICEBERG_REST),
        // token endpoint is relative => Iceberg dialect
        Arguments.of(
            Map.of(TOKEN_ENDPOINT, "tokens", CLIENT_SECRET, "secret"), Dialect.ICEBERG_REST),
        // client secret present without client ID => Iceberg dialect
        Arguments.of(
            Map.of(ISSUER_URL, "https://example.com", CLIENT_SECRET, "secret"),
            Dialect.ICEBERG_REST));
  }
}
