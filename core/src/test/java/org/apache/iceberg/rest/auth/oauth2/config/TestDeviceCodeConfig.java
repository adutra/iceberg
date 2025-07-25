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
import static org.apache.iceberg.rest.auth.oauth2.config.DeviceCodeConfig.ENDPOINT;
import static org.apache.iceberg.rest.auth.oauth2.config.DeviceCodeConfig.POLL_INTERVAL;
import static org.apache.iceberg.rest.auth.oauth2.config.DeviceCodeConfig.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDeviceCodeConfig {

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testValidate(Map<String, String> properties, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DeviceCodeConfig.fromProperties(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "/auth"),
            singletonList(
                "device code flow: device authorization endpoint must not be relative (rest.auth.oauth2.device-code.endpoint)")),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com?query"),
            singletonList(
                "device code flow: device authorization endpoint must not have a query part (rest.auth.oauth2.device-code.endpoint)")),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com#fragment"),
            singletonList(
                "device code flow: device authorization endpoint must not have a fragment part (rest.auth.oauth2.device-code.endpoint)")),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com", PREFIX + POLL_INTERVAL, "PT1S"),
            singletonList(
                "device code flow: poll interval must be greater than or equal to PT5S (rest.auth.oauth2.device-code.poll-interval)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(Map<String, String> properties, DeviceCodeConfig expected) {
    DeviceCodeConfig actual = DeviceCodeConfig.fromProperties(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableDeviceCodeConfig.builder().build()),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com/device"),
            ImmutableDeviceCodeConfig.builder()
                .deviceAuthorizationEndpoint(URI.create("https://example.com/device"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + POLL_INTERVAL, "PT10S"),
            ImmutableDeviceCodeConfig.builder().pollInterval(Duration.ofSeconds(10)).build()));
  }
}
