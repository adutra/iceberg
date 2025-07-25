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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOption;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOptions;
import org.apache.iceberg.rest.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public interface DeviceCodeConfig {

  Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);
  Duration MIN_POLL_INTERVAL = Duration.ofSeconds(5); // mandated by the specs

  DeviceCodeConfig DEFAULT = builder().build();

  /**
   * The OAuth2 device authorization endpoint. Either this or {@link BasicConfig#issuerUrl()} must
   * be set, if the grant type is {@link GrantType#DEVICE_CODE}. This is the endpoint where the
   * device authorization request will be sent to.
   *
   * @see OAuth2Properties.DeviceCode#ENDPOINT
   */
  Optional<URI> deviceAuthorizationEndpoint();

  /**
   * How often to poll the token endpoint. Defaults to {@link #DEFAULT_POLL_INTERVAL}. Only relevant
   * when using the {@link GrantType#DEVICE_CODE} grant type.
   *
   * @see OAuth2Properties.DeviceCode#POLL_INTERVAL
   */
  @Value.Default
  default Duration pollInterval() {
    return DEFAULT_POLL_INTERVAL;
  }

  /**
   * The minimum poll interval for the device code flow. The device code flow requires a minimum
   * poll interval of 5 seconds.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default Duration minPollInterval() {
    return MIN_POLL_INTERVAL;
  }

  /**
   * Whether to ignore the server-specified poll interval and always use the configured poll
   * interval.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default boolean ignoreServerPollInterval() {
    return false;
  }

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (deviceAuthorizationEndpoint().isPresent()) {
      validator.checkEndpoint(
          deviceAuthorizationEndpoint().get(),
          true,
          OAuth2Properties.DeviceCode.ENDPOINT,
          "device code flow: device authorization endpoint %s");
    }

    validator.check(
        pollInterval().compareTo(minPollInterval()) >= 0,
        OAuth2Properties.DeviceCode.POLL_INTERVAL,
        "device code flow: poll interval must be greater than or equal to %s",
        minPollInterval());
    validator.validate();
  }

  /** Merges the given properties into this {@link DeviceCodeConfig} and returns the result. */
  default DeviceCodeConfig merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Builder builder = builder();
    builder.deviceAuthorizationEndpointOption().set(properties, deviceAuthorizationEndpoint());
    builder.pollIntervalOption().set(properties, pollInterval());
    builder.minPollInterval(minPollInterval());
    builder.ignoreServerPollInterval(ignoreServerPollInterval());
    return builder.build();
  }

  static Builder builder() {
    return ImmutableDeviceCodeConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(DeviceCodeConfig config);

    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      deviceAuthorizationEndpointOption().set(properties);
      pollIntervalOption().set(properties);
      return this;
    }

    @CanIgnoreReturnValue
    Builder deviceAuthorizationEndpoint(URI deviceAuthorizationEndpoint);

    @CanIgnoreReturnValue
    Builder pollInterval(Duration pollInterval);

    @CanIgnoreReturnValue
    Builder minPollInterval(Duration minPollInterval);

    @CanIgnoreReturnValue
    Builder ignoreServerPollInterval(boolean ignoreServerPollInterval);

    DeviceCodeConfig build();

    private ConfigOption<URI> deviceAuthorizationEndpointOption() {
      return ConfigOptions.simple(
          OAuth2Properties.DeviceCode.ENDPOINT, this::deviceAuthorizationEndpoint, URI::create);
    }

    private ConfigOption<Duration> pollIntervalOption() {
      return ConfigOptions.simple(
          OAuth2Properties.DeviceCode.POLL_INTERVAL, this::pollInterval, Duration::parse);
    }
  }
}
