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

class TestPkceTransformation {

  @ParameterizedTest
  @MethodSource("configNameTestCases")
  void testFromConfigName(String configName, PkceTransformation expectedPkceTransformation) {
    assertThat(PkceTransformation.fromConfigName(configName)).isEqualTo(expectedPkceTransformation);
  }

  @Test
  void testFromConfigNameInvalidName() {
    assertThatThrownBy(() -> PkceTransformation.fromConfigName(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid PKCE transformation name: null");
    assertThatThrownBy(() -> PkceTransformation.fromConfigName(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown PKCE transformation: ");
    assertThatThrownBy(() -> PkceTransformation.fromConfigName("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown PKCE transformation: invalid");
  }

  static Stream<Arguments> configNameTestCases() {
    return Stream.of(
        Arguments.of("S256", PkceTransformation.S256),
        Arguments.of("plain", PkceTransformation.PLAIN),
        Arguments.of("S256", PkceTransformation.S256),
        Arguments.of("PLAIN", PkceTransformation.PLAIN),
        Arguments.of("s256", PkceTransformation.S256),
        Arguments.of("Plain", PkceTransformation.PLAIN));
  }
}
