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

import static org.apache.iceberg.rest.oauth2.OAuth2Properties.ResourceOwnerPassword.PASSWORD;
import static org.apache.iceberg.rest.oauth2.OAuth2Properties.ResourceOwnerPassword.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestResourceOwnerPasswordConfig {

  @ParameterizedTest
  @MethodSource
  void testFromProperties(
      Map<String, String> properties,
      ResourceOwnerPasswordConfig expected,
      Throwable expectedThrowable) {
    if (expectedThrowable == null) {
      ResourceOwnerPasswordConfig actual =
          ResourceOwnerPasswordConfig.builder().from(properties).build();
      assertThat(actual)
          .usingRecursiveComparison()
          .ignoringFields("clientSecretProvider")
          .isEqualTo(expected);
    } else {
      Throwable actual =
          catchThrowable(() -> ResourceOwnerPasswordConfig.builder().from(properties));
      assertThat(actual)
          .isInstanceOf(expectedThrowable.getClass())
          .hasMessage(expectedThrowable.getMessage());
    }
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(null, null, new NullPointerException("Invalid properties map: null")),
        Arguments.of(
            Map.of(USERNAME, "Alice", PASSWORD, "s3cr3t"),
            ResourceOwnerPasswordConfig.builder().username("Alice").password("s3cr3t").build(),
            null));
  }

  @ParameterizedTest
  @MethodSource
  void testMerge(
      ResourceOwnerPasswordConfig base,
      Map<String, String> properties,
      ResourceOwnerPasswordConfig expected) {
    ResourceOwnerPasswordConfig merged = base.merge(properties);
    assertThat(merged).isEqualTo(expected);
  }

  static Stream<Arguments> testMerge() {
    return Stream.of(
        emptyBase(), emptyProperties(), nonEmptyBaseNonEmptyProperties(), baseCleared());
  }

  private static Arguments emptyBase() {
    return Arguments.of(
        ResourceOwnerPasswordConfig.builder().build(),
        Map.of(USERNAME, "Alice", PASSWORD, "s3cr3t"),
        ResourceOwnerPasswordConfig.builder().username("Alice").password("s3cr3t").build());
  }

  private static Arguments emptyProperties() {
    return Arguments.of(
        ResourceOwnerPasswordConfig.builder().username("Alice").password("s3cr3t").build(),
        Map.of(),
        ResourceOwnerPasswordConfig.builder().username("Alice").password("s3cr3t").build());
  }

  private static Arguments nonEmptyBaseNonEmptyProperties() {
    return Arguments.of(
        ResourceOwnerPasswordConfig.builder().username("Alice").password("s3cr3t").build(),
        Map.of(USERNAME, "Bob", PASSWORD, "w00t"),
        ResourceOwnerPasswordConfig.builder().username("Bob").password("w00t").build());
  }

  private static Arguments baseCleared() {
    return Arguments.of(
        ResourceOwnerPasswordConfig.builder().username("Alice").password("s3cr3t").build(),
        Map.of(USERNAME, "", PASSWORD, ""),
        ResourceOwnerPasswordConfig.DEFAULT);
  }
}
