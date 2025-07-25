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

import com.nimbusds.oauth2.sdk.GrantType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/** Configuration properties for the token refresh feature. */
@Value.Immutable
public interface TokenRefreshConfig {

  String GROUP_NAME = "token-refresh";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String ENABLED = "enabled";
  String GRANT_TYPE = "grant-type";
  String ACCESS_TOKEN_LIFESPAN = "access-token-lifespan";
  String SAFETY_MARGIN = "safety-margin";
  String IDLE_TIMEOUT = "idle-timeout";

  Duration DEFAULT_ACCESS_TOKEN_LIFESPAN = Duration.ofMinutes(5);
  Duration DEFAULT_SAFETY_MARGIN = Duration.ofSeconds(10);
  Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(30);

  /**
   * Whether to enable token refresh. If enabled, the OAuth2 client will automatically refresh its
   * access token when it expires. If disabled, the OAuth2 client will only fetch the initial access
   * token, but won't refresh it. Defaults to {@code true}.
   */
  @ConfigOption(ENABLED)
  @Value.Default
  default boolean enabled() {
    return true;
  }

  /**
   * The grant type to use when refreshing the access token. Valid values are:
   *
   * <ul>
   *   <li>{@link GrantType#REFRESH_TOKEN refresh_token}: uses the refresh token to obtain a new
   *       access token.
   *   <li>{@link GrantType#TOKEN_EXCHANGE urn:ietf:params:oauth:grant-type:token-exchange}: uses
   *       the token exchange grant type to obtain a new access token.
   * </ul>
   *
   * <p>Optional, defaults to {@link GrantType#TOKEN_EXCHANGE} for backwards compatibility reasons.
   * When using strict OAuth2 providers, this grant type may not be supported, in which case the
   * {@link GrantType#REFRESH_TOKEN} grant type should be selected instead.
   */
  @ConfigOption(GRANT_TYPE)
  @Value.Default
  default GrantType grantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  /**
   * Default access token lifespan; if the OAuth2 server returns an access token without specifying
   * its expiration time, this value will be used. Note that when this happens, a warning will be
   * logged.
   *
   * <p>Optional, defaults to {@link #DEFAULT_ACCESS_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigOption(ACCESS_TOKEN_LIFESPAN)
  @Value.Default
  default Duration accessTokenLifespan() {
    return DEFAULT_ACCESS_TOKEN_LIFESPAN;
  }

  /**
   * Refresh safety margin to use; a new token will be fetched when the current token's remaining
   * lifespan is less than this value. Optional, defaults to {@link #DEFAULT_SAFETY_MARGIN}. Must be
   * a valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigOption(SAFETY_MARGIN)
  @Value.Default
  default Duration safetyMargin() {
    return DEFAULT_SAFETY_MARGIN;
  }

  /**
   * Defines for how long the OAuth2 client should keep the tokens fresh, if it is not being
   * actively used.
   *
   * <p>Setting this value too high may cause an excessive usage of network I/O and thread
   * resources; conversely, when setting it too low, if the OAuth2 client is used again, the calling
   * thread may block if the tokens are expired and need to be renewed synchronously.
   *
   * <p>Optional, defaults to {@link #DEFAULT_IDLE_TIMEOUT}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigOption(IDLE_TIMEOUT)
  @Value.Default
  default Duration idleTimeout() {
    return DEFAULT_IDLE_TIMEOUT;
  }

  /**
   * The minimum access token lifespan.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  @Value.Default
  default Duration minAccessTokenLifespan() {
    return Duration.ofSeconds(30);
  }

  /**
   * The minimum delay between two token refreshes.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  @Value.Default
  default Duration minRefreshDelay() {
    return Duration.ofSeconds(5);
  }

  /**
   * The minimum OAuth2 client idle timeout.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  @Value.Default
  default Duration minIdleTimeout() {
    return Duration.ofSeconds(30);
  }

  @Value.Check
  default void validate() {
    if (enabled()) {
      ConfigValidator validator = new ConfigValidator();
      validator.check(
          ConfigUtils.SUPPORTED_REFRESH_GRANT_TYPES.contains(grantType()),
          PREFIX + GRANT_TYPE,
          "refresh grant type must be one of: %s",
          ConfigUtils.SUPPORTED_REFRESH_GRANT_TYPES.stream()
              .map(GrantType::getValue)
              .collect(Collectors.joining("', '", "'", "'")));
      validator.check(
          accessTokenLifespan().compareTo(minAccessTokenLifespan()) >= 0,
          PREFIX + ACCESS_TOKEN_LIFESPAN,
          "access token lifespan must be greater than or equal to %s",
          minAccessTokenLifespan());
      validator.check(
          safetyMargin().compareTo(minRefreshDelay()) >= 0,
          PREFIX + SAFETY_MARGIN,
          "refresh safety margin must be greater than or equal to %s",
          minRefreshDelay());
      validator.check(
          safetyMargin().compareTo(accessTokenLifespan()) < 0,
          List.of(PREFIX + SAFETY_MARGIN, PREFIX + ACCESS_TOKEN_LIFESPAN),
          "refresh safety margin must be less than the access token lifespan");
      validator.check(
          idleTimeout().compareTo(minIdleTimeout()) >= 0,
          PREFIX + IDLE_TIMEOUT,
          "token refresh idle timeout must be greater than or equal to %s",
          minIdleTimeout());
      validator.validate();
    }
  }

  static ImmutableTokenRefreshConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    return ImmutableTokenRefreshConfig.builder()
        .enabled(ConfigUtils.parseOptional(props, ENABLED, Boolean::parseBoolean).orElse(true))
        .grantType(
            ConfigUtils.parseOptional(props, GRANT_TYPE, GrantType::parse)
                .orElse(GrantType.TOKEN_EXCHANGE))
        .accessTokenLifespan(
            ConfigUtils.parseOptional(props, ACCESS_TOKEN_LIFESPAN, Duration::parse)
                .orElse(DEFAULT_ACCESS_TOKEN_LIFESPAN))
        .safetyMargin(
            ConfigUtils.parseOptional(props, SAFETY_MARGIN, Duration::parse)
                .orElse(DEFAULT_SAFETY_MARGIN))
        .idleTimeout(
            ConfigUtils.parseOptional(props, IDLE_TIMEOUT, Duration::parse)
                .orElse(DEFAULT_IDLE_TIMEOUT));
  }
}
