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

import static org.apache.iceberg.rest.auth.oauth2.config.ConfigMigrator.DEFAULT_CLIENT_ID;
import static org.apache.iceberg.rest.auth.oauth2.config.ConfigMigrator.MESSAGE_TEMPLATE;
import static org.apache.iceberg.rest.auth.oauth2.config.ConfigMigrator.MESSAGE_TEMPLATE_NO_CLIENT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.array;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class TestConfigMigrator {

  private List<Pair<String, String[]>> messages;
  private BiConsumer<String, String[]> consumer;

  @BeforeEach
  void before() {
    messages = Lists.newArrayList();
    consumer = (msg, args) -> messages.add(Pair.of(msg, args));
  }

  @AfterEach
  void after() {
    messages.clear();
  }

  @Test
  void emptyMap() {
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(Map.of());
    assertThat(actual).isEmpty();
    assertThat(messages).isEmpty();
  }

  @Test
  void noLegacyProperties() {
    Map<String, String> input =
        Map.of(
            OAuth2Config.PREFIX + BasicConfig.CLIENT_ID,
            "client1",
            OAuth2Config.PREFIX + BasicConfig.CLIENT_SECRET,
            "secret",
            "non.oauth2.property",
            "value");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    // Only OAuth2 properties should be included
    assertThat(actual)
        .isEqualTo(
            Map.of(
                OAuth2Config.PREFIX + BasicConfig.CLIENT_ID, "client1",
                OAuth2Config.PREFIX + BasicConfig.CLIENT_SECRET, "secret"));
    assertThat(messages).isEmpty();
  }

  @Test
  void credentialValid() {
    Map<String, String> input = Map.of(OAuth2Properties.CREDENTIAL, "client1:secret1");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                OAuth2Config.PREFIX + BasicConfig.CLIENT_ID, "client1",
                OAuth2Config.PREFIX + BasicConfig.CLIENT_SECRET, "secret1"));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.CREDENTIAL,
            "s",
            BasicConfig.PREFIX
                + BasicConfig.CLIENT_ID
                + " and "
                + BasicConfig.PREFIX
                + BasicConfig.CLIENT_SECRET);
  }

  @Test
  void credentialNoClientId() {
    Map<String, String> input = Map.of(OAuth2Properties.CREDENTIAL, "secret1");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                OAuth2Config.PREFIX + BasicConfig.CLIENT_ID,
                DEFAULT_CLIENT_ID,
                OAuth2Config.PREFIX + BasicConfig.CLIENT_SECRET,
                "secret1"));
    assertThat(messages).hasSize(2);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.CREDENTIAL,
            "s",
            BasicConfig.PREFIX
                + BasicConfig.CLIENT_ID
                + " and "
                + BasicConfig.PREFIX
                + BasicConfig.CLIENT_SECRET);
    Pair<String, String[]> message2 = messages.get(1);
    assertThat(message2).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE_NO_CLIENT_ID);
  }

  @Test
  void token() {
    Map<String, String> input = Map.of(OAuth2Properties.TOKEN, "access-token-123");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(Map.of(BasicConfig.PREFIX + BasicConfig.TOKEN, "access-token-123"));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(OAuth2Properties.TOKEN, "", BasicConfig.PREFIX + BasicConfig.TOKEN);
  }

  @Test
  void tokenExpiresInMs() {
    Map<String, String> input = Map.of(OAuth2Properties.TOKEN_EXPIRES_IN_MS, "300000");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN,
                Duration.ofMillis(300000).toString()));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.TOKEN_EXPIRES_IN_MS,
            "",
            TokenRefreshConfig.PREFIX + TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN);
  }

  @CartesianTest
  void tokenRefreshEnabled(@Values(booleans = {true, false}) boolean enabled) {
    Map<String, String> input =
        Map.of(OAuth2Properties.TOKEN_REFRESH_ENABLED, String.valueOf(enabled));
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.ENABLED, String.valueOf(enabled)));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            "",
            TokenRefreshConfig.PREFIX + TokenRefreshConfig.ENABLED);
  }

  @Test
  void oauth2ServerUri() {
    Map<String, String> input =
        Map.of(OAuth2Properties.OAUTH2_SERVER_URI, "https://example.com/token");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(BasicConfig.PREFIX + BasicConfig.TOKEN_ENDPOINT, "https://example.com/token"));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.OAUTH2_SERVER_URI,
            "s",
            BasicConfig.PREFIX
                + BasicConfig.ISSUER_URL
                + " or "
                + BasicConfig.PREFIX
                + BasicConfig.TOKEN_ENDPOINT);
  }

  @Test
  void scope() {
    Map<String, String> input = Map.of(OAuth2Properties.SCOPE, "read write admin");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(Map.of(BasicConfig.PREFIX + BasicConfig.SCOPE, "read write admin"));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(OAuth2Properties.SCOPE, "", BasicConfig.PREFIX + BasicConfig.SCOPE);
  }

  @Test
  void audience() {
    Map<String, String> input = Map.of(OAuth2Properties.AUDIENCE, "https://api.example.com");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES,
                "https://api.example.com"));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.AUDIENCE,
            "",
            TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES);
  }

  @Test
  void resource() {
    Map<String, String> input = Map.of(OAuth2Properties.RESOURCE, "urn:example:resource");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE, "urn:example:resource"));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.RESOURCE,
            "",
            TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE);
  }

  @ParameterizedTest
  @MethodSource
  void vendedTokenExchange(String tokenTypeProperty) {
    Map<String, String> input = Map.of(tokenTypeProperty, "some-value");
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                BasicConfig.PREFIX + BasicConfig.GRANT_TYPE,
                GrantType.TOKEN_EXCHANGE.getValue(),
                TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN,
                "some-value",
                TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN_TYPE,
                tokenTypeProperty));

    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            tokenTypeProperty,
            "s",
            BasicConfig.PREFIX
                + BasicConfig.GRANT_TYPE
                + ", "
                + TokenExchangeConfig.PREFIX
                + TokenExchangeConfig.SUBJECT_TOKEN
                + " and "
                + TokenExchangeConfig.PREFIX
                + TokenExchangeConfig.SUBJECT_TOKEN_TYPE);
  }

  @CartesianTest
  void tokenExchangeEnabled(@Values(booleans = {true, false}) boolean enabled) {
    Map<String, String> input =
        Map.of(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, String.valueOf(enabled));
    GrantType expectedGrantType = enabled ? GrantType.TOKEN_EXCHANGE : GrantType.REFRESH_TOKEN;
    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.GRANT_TYPE,
                expectedGrantType.getValue()));
    assertThat(messages).hasSize(1);
    Pair<String, String[]> message = messages.get(0);
    assertThat(message).extracting(Pair::first).isEqualTo(MESSAGE_TEMPLATE);
    assertThat(message)
        .extracting(Pair::second)
        .asInstanceOf(array(String[].class))
        .containsExactly(
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
            "",
            TokenRefreshConfig.PREFIX + TokenRefreshConfig.GRANT_TYPE);
  }

  static Stream<String> vendedTokenExchange() {
    return Stream.of(
        OAuth2Properties.ACCESS_TOKEN_TYPE,
        OAuth2Properties.ID_TOKEN_TYPE,
        OAuth2Properties.SAML1_TOKEN_TYPE,
        OAuth2Properties.SAML2_TOKEN_TYPE,
        OAuth2Properties.JWT_TOKEN_TYPE,
        OAuth2Properties.REFRESH_TOKEN_TYPE);
  }

  @Test
  void fullMigrationScenario() {
    Map<String, String> input =
        ImmutableMap.<String, String>builder()
            .put(OAuth2Properties.CREDENTIAL, "client1:secret1")
            .put(OAuth2Properties.TOKEN, "access-token")
            .put(OAuth2Properties.TOKEN_EXPIRES_IN_MS, "300000")
            .put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "true")
            .put(OAuth2Properties.OAUTH2_SERVER_URI, "https://example.com/token")
            .put(OAuth2Properties.SCOPE, "read write")
            .put(OAuth2Properties.AUDIENCE, "https://api.example.com")
            .put(OAuth2Properties.RESOURCE, "urn:example:resource")
            .put(OAuth2Properties.JWT_TOKEN_TYPE, "vended-jwt")
            .put(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "false")
            .put(
                BasicConfig.PREFIX + BasicConfig.ISSUER_URL,
                "https://example.com") // New property should be preserved
            .put("non.oauth2.property", "ignored") // Non-OAuth2 property should be filtered out
            .build();

    Map<String, String> expected =
        ImmutableMap.<String, String>builder()
            .put(BasicConfig.PREFIX + BasicConfig.GRANT_TYPE, GrantType.TOKEN_EXCHANGE.getValue())
            .put(BasicConfig.PREFIX + BasicConfig.CLIENT_ID, "client1")
            .put(BasicConfig.PREFIX + BasicConfig.CLIENT_SECRET, "secret1")
            .put(BasicConfig.PREFIX + BasicConfig.TOKEN, "access-token")
            .put(BasicConfig.PREFIX + BasicConfig.TOKEN_ENDPOINT, "https://example.com/token")
            .put(BasicConfig.PREFIX + BasicConfig.ISSUER_URL, "https://example.com")
            .put(BasicConfig.PREFIX + BasicConfig.SCOPE, "read write")
            .put(TokenRefreshConfig.PREFIX + TokenRefreshConfig.ENABLED, "true")
            .put(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.GRANT_TYPE,
                GrantType.REFRESH_TOKEN.getValue())
            .put(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN,
                Duration.ofMillis(300000).toString())
            .put(TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN, "vended-jwt")
            .put(
                TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN_TYPE,
                TokenTypeURI.JWT.toString())
            .put(TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE, "urn:example:resource")
            .put(
                TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES,
                "https://api.example.com")
            .build();

    Map<String, String> actual = new ConfigMigrator(consumer).migrate(input);
    assertThat(actual).isEqualTo(expected);

    assertThat(messages).hasSize(10);
    List<String> legacyProperties =
        messages.stream().map(Pair::second).map(args -> args[0]).collect(Collectors.toList());

    assertThat(legacyProperties)
        .containsExactlyInAnyOrder(
            OAuth2Properties.CREDENTIAL,
            OAuth2Properties.TOKEN,
            OAuth2Properties.TOKEN_EXPIRES_IN_MS,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.OAUTH2_SERVER_URI,
            OAuth2Properties.SCOPE,
            OAuth2Properties.AUDIENCE,
            OAuth2Properties.RESOURCE,
            OAuth2Properties.JWT_TOKEN_TYPE,
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED);
  }

  @Test
  void noDuplicateWarnings() {
    Map<String, String> input =
        ImmutableMap.<String, String>builder()
            .put(OAuth2Properties.CREDENTIAL, "client1:secret1")
            .put(OAuth2Properties.TOKEN, "access-token")
            .put(OAuth2Properties.TOKEN_EXPIRES_IN_MS, "300000")
            .put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "true")
            .put(OAuth2Properties.OAUTH2_SERVER_URI, "https://example.com/token")
            .put(OAuth2Properties.SCOPE, "read write")
            .put(OAuth2Properties.AUDIENCE, "https://api.example.com")
            .put(OAuth2Properties.RESOURCE, "urn:example:resource")
            .put(OAuth2Properties.ACCESS_TOKEN_TYPE, "vended-access-token")
            .put(OAuth2Properties.ID_TOKEN_TYPE, "vended-id-token")
            .put(OAuth2Properties.SAML1_TOKEN_TYPE, "vended-saml1-token")
            .put(OAuth2Properties.SAML2_TOKEN_TYPE, "vended-saml2-token")
            .put(OAuth2Properties.JWT_TOKEN_TYPE, "vended-jwt-token")
            .put(OAuth2Properties.REFRESH_TOKEN_TYPE, "vended-refresh-token")
            .put(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "false")
            .build();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    migrator.migrate(input);
    migrator.migrate(input);
    assertThat(messages).hasSize(input.size());
  }
}
