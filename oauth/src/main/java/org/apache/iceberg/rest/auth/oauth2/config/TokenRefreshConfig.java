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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.config.option.ConfigOption;
import org.apache.iceberg.rest.auth.oauth2.config.option.ConfigOptions;
import org.apache.iceberg.rest.auth.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public interface TokenRefreshConfig {

  Duration DEFAULT_ACCESS_TOKEN_LIFESPAN = Duration.ofMinutes(5);
  Duration DEFAULT_SAFETY_MARGIN = Duration.ofSeconds(10);
  Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(30);

  Duration MIN_ACCESS_TOKEN_LIFESPAN = Duration.ofSeconds(30);
  Duration MIN_IDLE_TIMEOUT = Duration.ofSeconds(30);
  Duration MIN_REFRESH_DELAY = Duration.ofSeconds(5);

  TokenRefreshConfig DEFAULT = builder().build();

  /**
   * Whether token refresh is enabled. If enabled, the agent will automatically refresh the access
   * token when it expires. If disabled, the agent will only fetch the initial access token, but
   * won't refresh it. Optional, defaults to {@code true}.
   *
   * @see OAuth2Properties.TokenRefresh#ENABLED
   */
  @Value.Default
  default boolean enabled() {
    return true;
  }

  /**
   * The default access token lifespan; if the OAuth2 server returns an access token without
   * specifying its expiration time, this value will be used. Note that when this happens, a warning
   * will be logged. Optional, defaults to {@link #DEFAULT_ACCESS_TOKEN_LIFESPAN}.
   *
   * @see OAuth2Properties.TokenRefresh#ACCESS_TOKEN_LIFESPAN
   */
  @Value.Default
  default Duration accessTokenLifespan() {
    return DEFAULT_ACCESS_TOKEN_LIFESPAN;
  }

  /**
   * The refresh safety margin. A new token will be fetched when the current token's remaining
   * lifespan is less than this value. Optional, defaults to {@link #DEFAULT_SAFETY_MARGIN}.
   *
   * @see OAuth2Properties.TokenRefresh#SAFETY_MARGIN
   */
  @Value.Default
  default Duration safetyMargin() {
    return DEFAULT_SAFETY_MARGIN;
  }

  /**
   * For how long the OAuth2 client should keep the tokens fresh, if the agent is not being actively
   * used. Defaults to {@link #DEFAULT_IDLE_TIMEOUT}.
   *
   * @see OAuth2Properties.TokenRefresh#IDLE_TIMEOUT
   */
  @Value.Default
  default Duration idleTimeout() {
    return DEFAULT_IDLE_TIMEOUT;
  }

  /**
   * The minimum access token lifespan. Optional, defaults to {@code 30 seconds}.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default Duration minAccessTokenLifespan() {
    return MIN_ACCESS_TOKEN_LIFESPAN;
  }

  /**
   * The minimum refresh safety margin. Optional, defaults to {@code 5 seconds}.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default Duration minRefreshDelay() {
    return MIN_REFRESH_DELAY;
  }

  /**
   * The minimum token refresh idle timeout. Optional, defaults to {@code 30 seconds}.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default Duration minIdleTimeout() {
    return MIN_IDLE_TIMEOUT;
  }

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    validator.check(
        accessTokenLifespan().compareTo(minAccessTokenLifespan()) >= 0,
        OAuth2Properties.TokenRefresh.ACCESS_TOKEN_LIFESPAN,
        "access token lifespan must be greater than or equal to %s",
        minAccessTokenLifespan());
    validator.check(
        safetyMargin().compareTo(minRefreshDelay()) >= 0,
        OAuth2Properties.TokenRefresh.SAFETY_MARGIN,
        "refresh safety margin must be greater than or equal to %s",
        minRefreshDelay());
    validator.check(
        safetyMargin().compareTo(accessTokenLifespan()) < 0,
        List.of(
            OAuth2Properties.TokenRefresh.SAFETY_MARGIN,
            OAuth2Properties.TokenRefresh.ACCESS_TOKEN_LIFESPAN),
        "refresh safety margin must be less than the access token lifespan");
    validator.check(
        idleTimeout().compareTo(minIdleTimeout()) >= 0,
        OAuth2Properties.TokenRefresh.IDLE_TIMEOUT,
        "token refresh idle timeout must be greater than or equal to %s",
        minIdleTimeout());
    validator.validate();
  }

  /** Merges the given properties into this {@link TokenRefreshConfig} and returns the result. */
  default TokenRefreshConfig merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Builder builder = builder();
    builder.enabledOption().set(properties, enabled());
    builder.accessTokenLifespanOption().set(properties, accessTokenLifespan());
    builder.safetyMarginOption().set(properties, safetyMargin());
    builder.idleTimeoutOption().set(properties, idleTimeout());
    builder.minAccessTokenLifespan(minAccessTokenLifespan());
    builder.minRefreshDelay(minRefreshDelay());
    builder.minIdleTimeout(minIdleTimeout());
    return builder.build();
  }

  static Builder builder() {
    return ImmutableTokenRefreshConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(TokenRefreshConfig config);

    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      enabledOption().set(properties);
      accessTokenLifespanOption().set(properties);
      safetyMarginOption().set(properties);
      idleTimeoutOption().set(properties);
      return this;
    }

    @CanIgnoreReturnValue
    Builder enabled(boolean enabled);

    @CanIgnoreReturnValue
    Builder accessTokenLifespan(Duration accessTokenLifespan);

    @CanIgnoreReturnValue
    Builder safetyMargin(Duration safetyMargin);

    @CanIgnoreReturnValue
    Builder idleTimeout(Duration idleTimeout);

    Builder minAccessTokenLifespan(Duration minAccessTokenLifespan);

    @CanIgnoreReturnValue
    Builder minRefreshDelay(Duration minRefreshDelay);

    @CanIgnoreReturnValue
    Builder minIdleTimeout(Duration minIdleTimeout);

    TokenRefreshConfig build();

    private ConfigOption<Boolean> enabledOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenRefresh.ENABLED, this::enabled, Boolean::parseBoolean);
    }

    private ConfigOption<Duration> accessTokenLifespanOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenRefresh.ACCESS_TOKEN_LIFESPAN,
          this::accessTokenLifespan,
          Duration::parse);
    }

    private ConfigOption<Duration> safetyMarginOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenRefresh.SAFETY_MARGIN, this::safetyMargin, Duration::parse);
    }

    private ConfigOption<Duration> idleTimeoutOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenRefresh.IDLE_TIMEOUT, this::idleTimeout, Duration::parse);
    }
  }
}
