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

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Configuration properties for the <a href="https://datatracker.ietf.org/doc/html/rfc8628">Device
 * Authorization Grant</a> flow.
 *
 * <p>This flow is used to obtain an access token for devices that do not have a browser or limited
 * input capabilities. The user is prompted to visit a URL on another device and enter a code to
 * authorize the device.
 */
@Value.Immutable
public interface DeviceCodeConfig {

  String GROUP_NAME = "device-code";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String ENDPOINT = "endpoint";
  String POLL_INTERVAL = "poll-interval";

  Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);

  /**
   * URL of the OAuth2 device authorization endpoint. For Keycloak, this is typically {@code
   * http://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/auth/device}.
   *
   * <p>If using the "Device Code" grant type, either this property or {@link
   * BasicConfig#ISSUER_URL} must be set.
   */
  @ConfigOption(ENDPOINT)
  Optional<URI> deviceAuthorizationEndpoint();

  /**
   * Defines how often the OAuth2 client should poll the OAuth2 server for the device code flow to
   * complete.
   *
   * <p>Optional, defaults to {@link #DEFAULT_POLL_INTERVAL}.
   *
   * <p>Must be a valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601
   * duration</a>.
   */
  @ConfigOption(POLL_INTERVAL)
  @Value.Default
  default Duration pollInterval() {
    return DEFAULT_POLL_INTERVAL;
  }

  /**
   * Minimum poll interval.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  @Value.Default
  default Duration minPollInterval() {
    return Duration.ofSeconds(5);
  }

  /**
   * Whether to ignore the server's requested poll interval.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  @Value.Default
  default boolean ignoreServerPollInterval() {
    return false;
  }

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (deviceAuthorizationEndpoint().isPresent()) {
      validator.checkEndpoint(
          deviceAuthorizationEndpoint().get(),
          PREFIX + ENDPOINT,
          "device code flow: device authorization endpoint");
    }

    validator.check(
        pollInterval().compareTo(minPollInterval()) >= 0,
        PREFIX + POLL_INTERVAL,
        "device code flow: poll interval must be greater than or equal to %s",
        minPollInterval());
    validator.validate();
  }

  static ImmutableDeviceCodeConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    return ImmutableDeviceCodeConfig.builder()
        .deviceAuthorizationEndpoint(ConfigUtils.parseOptional(props, ENDPOINT, URI::create))
        .pollInterval(
            ConfigUtils.parseOptional(props, POLL_INTERVAL, Duration::parse)
                .orElse(DEFAULT_POLL_INTERVAL));
  }
}
