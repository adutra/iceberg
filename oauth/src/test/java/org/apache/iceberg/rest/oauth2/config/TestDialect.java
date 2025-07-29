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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDialect {

  @ParameterizedTest
  @MethodSource("configNameTestCases")
  void testFromConfigName(String configName, Dialect expectedDialect) {
    assertThat(Dialect.fromConfigName(configName)).isEqualTo(expectedDialect);
  }

  @Test
  void testFromConfigNameInvalidName() {
    assertThatThrownBy(() -> Dialect.fromConfigName(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid OAuth2 dialect: null");
    assertThatThrownBy(() -> Dialect.fromConfigName(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown OAuth2 dialect: ");
    assertThatThrownBy(() -> Dialect.fromConfigName("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown OAuth2 dialect: invalid");
  }

  static Stream<Arguments> configNameTestCases() {
    return Stream.of(
        Arguments.of("standard", Dialect.STANDARD),
        Arguments.of("iceberg_rest", Dialect.ICEBERG_REST),
        Arguments.of("STANDARD", Dialect.STANDARD),
        Arguments.of("ICEBERG_REST", Dialect.ICEBERG_REST),
        Arguments.of("Standard", Dialect.STANDARD));
  }
}
