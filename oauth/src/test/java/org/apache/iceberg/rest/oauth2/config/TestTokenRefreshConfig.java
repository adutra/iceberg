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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.TokenRefresh.ACCESS_TOKEN_LIFESPAN;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.TokenRefresh.ENABLED;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.TokenRefresh.IDLE_TIMEOUT;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.TokenRefresh.SAFETY_MARGIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.rest.oauth2.config.validator.ConfigValidator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTokenRefreshConfig {

  @ParameterizedTest
  @MethodSource
  void testValidate(TokenRefreshConfig.Builder config, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(config::build)
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            TokenRefreshConfig.builder().accessTokenLifespan(Duration.ofSeconds(2)),
            asList(
                "access token lifespan must be greater than or equal to PT30S (rest.auth.oauth2.token-refresh.access-token-lifespan)",
                "refresh safety margin must be less than the access token lifespan (rest.auth.oauth2.token-refresh.safety-margin / rest.auth.oauth2.token-refresh.access-token-lifespan)")),
        Arguments.of(
            TokenRefreshConfig.builder().safetyMargin(Duration.ofMillis(100)),
            singletonList(
                "refresh safety margin must be greater than or equal to PT5S (rest.auth.oauth2.token-refresh.safety-margin)")),
        Arguments.of(
            TokenRefreshConfig.builder()
                .safetyMargin(Duration.ofMinutes(10))
                .accessTokenLifespan(Duration.ofMinutes(5)),
            singletonList(
                "refresh safety margin must be less than the access token lifespan (rest.auth.oauth2.token-refresh.safety-margin / rest.auth.oauth2.token-refresh.access-token-lifespan)")),
        Arguments.of(
            TokenRefreshConfig.builder().idleTimeout(Duration.ofMillis(100)),
            singletonList(
                "token refresh idle timeout must be greater than or equal to PT30S (rest.auth.oauth2.token-refresh.idle-timeout)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(
      Map<String, String> properties, TokenRefreshConfig expected, Throwable expectedThrowable) {
    if (properties != null && expected != null) {
      TokenRefreshConfig actual = TokenRefreshConfig.builder().from(properties).build();
      assertThat(actual).isEqualTo(expected);
    } else {
      Throwable actual = catchThrowable(() -> TokenRefreshConfig.builder().from(properties));
      assertThat(actual)
          .isInstanceOf(expectedThrowable.getClass())
          .hasMessage(expectedThrowable.getMessage());
    }
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(null, null, new NullPointerException("Invalid properties map: null")),
        Arguments.of(
            Map.of(
                ENABLED,
                "false",
                ACCESS_TOKEN_LIFESPAN,
                "PT1H",
                SAFETY_MARGIN,
                "PT10S",
                IDLE_TIMEOUT,
                "PT1M"),
            TokenRefreshConfig.builder()
                .enabled(false)
                .accessTokenLifespan(Duration.ofHours(1))
                .safetyMargin(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofMinutes(1))
                .build(),
            null));
  }

  @ParameterizedTest
  @MethodSource
  void testMerge(
      TokenRefreshConfig base, Map<String, String> properties, TokenRefreshConfig expected) {
    TokenRefreshConfig merged = base.merge(properties);
    assertThat(merged).isEqualTo(expected);
  }

  static Stream<Arguments> testMerge() {
    return Stream.of(
        emptyBase(), emptyProperties(), nonEmptyBaseNonEmptyProperties(), baseCleared());
  }

  private static Arguments emptyBase() {
    TokenRefreshConfig base = TokenRefreshConfig.builder().build();
    Map<String, String> properties =
        Map.of(
            ENABLED,
            "false",
            ACCESS_TOKEN_LIFESPAN,
            "PT1H",
            SAFETY_MARGIN,
            "PT10S",
            IDLE_TIMEOUT,
            "PT1M");
    TokenRefreshConfig expected =
        TokenRefreshConfig.builder()
            .enabled(false)
            .accessTokenLifespan(Duration.ofHours(1))
            .safetyMargin(Duration.ofSeconds(10))
            .idleTimeout(Duration.ofMinutes(1))
            .build();
    return Arguments.of(base, properties, expected);
  }

  private static Arguments emptyProperties() {
    TokenRefreshConfig base =
        TokenRefreshConfig.builder()
            .enabled(false)
            .accessTokenLifespan(Duration.ofHours(1))
            .safetyMargin(Duration.ofSeconds(10))
            .idleTimeout(Duration.ofMinutes(1))
            .build();
    return Arguments.of(base, Map.of(), base);
  }

  private static Arguments nonEmptyBaseNonEmptyProperties() {
    TokenRefreshConfig base =
        TokenRefreshConfig.builder()
            .enabled(false)
            .accessTokenLifespan(Duration.ofHours(1))
            .safetyMargin(Duration.ofSeconds(10))
            .idleTimeout(Duration.ofMinutes(1))
            .build();
    Map<String, String> properties =
        Map.of(
            ENABLED,
            "true",
            ACCESS_TOKEN_LIFESPAN,
            "PT2H",
            SAFETY_MARGIN,
            "PT20S",
            IDLE_TIMEOUT,
            "PT2M");
    TokenRefreshConfig expected =
        TokenRefreshConfig.builder()
            .enabled(true)
            .accessTokenLifespan(Duration.ofHours(2))
            .safetyMargin(Duration.ofSeconds(20))
            .idleTimeout(Duration.ofMinutes(2))
            .build();
    return Arguments.of(base, properties, expected);
  }

  private static Arguments baseCleared() {
    TokenRefreshConfig base =
        TokenRefreshConfig.builder()
            .enabled(false)
            .accessTokenLifespan(Duration.ofHours(1))
            .safetyMargin(Duration.ofSeconds(10))
            .idleTimeout(Duration.ofMinutes(1))
            .build();
    Map<String, String> properites =
        Map.of(ENABLED, "", ACCESS_TOKEN_LIFESPAN, "", SAFETY_MARGIN, "", IDLE_TIMEOUT, "");
    TokenRefreshConfig expected = TokenRefreshConfig.DEFAULT;
    return Arguments.of(base, properites, expected);
  }
}
