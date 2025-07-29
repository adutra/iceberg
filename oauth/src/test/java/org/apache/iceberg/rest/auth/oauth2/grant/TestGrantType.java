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
package org.apache.iceberg.rest.auth.oauth2.grant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestGrantType {

  @ParameterizedTest
  @MethodSource("configNameTestCases")
  void testFromConfigName(String configName, GrantType expectedGrantType) {
    assertThat(GrantType.fromConfigName(configName)).isEqualTo(expectedGrantType);
  }

  @Test
  void testFromConfigNameInvalidName() {
    assertThatThrownBy(() -> GrantType.fromConfigName(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid grant type: null");
    assertThatThrownBy(() -> GrantType.fromConfigName(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: ");
    assertThatThrownBy(() -> GrantType.fromConfigName("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid");
  }

  @ParameterizedTest
  @MethodSource("initialTestCases")
  void testInitial(GrantType grantType, boolean expectedInitial) {
    assertThat(grantType.initial()).isEqualTo(expectedInitial);
  }

  @ParameterizedTest
  @MethodSource("requiresUserInteractionTestCases")
  void testRequiresUserInteraction(GrantType grantType, boolean expectedRequiresUserInteraction) {
    assertThat(grantType.requiresUserInteraction()).isEqualTo(expectedRequiresUserInteraction);
  }

  static Stream<Arguments> configNameTestCases() {
    return Stream.of(
        // Test canonical names
        Arguments.of("client_credentials", GrantType.CLIENT_CREDENTIALS),
        Arguments.of("CLIENT_CREDENTIALS", GrantType.CLIENT_CREDENTIALS),
        Arguments.of("refresh_token", GrantType.REFRESH_TOKEN),
        Arguments.of("authorization_code", GrantType.AUTHORIZATION_CODE),
        Arguments.of("AUTHORIZATION_CODE", GrantType.AUTHORIZATION_CODE),
        Arguments.of("REFRESH_TOKEN", GrantType.REFRESH_TOKEN),
        Arguments.of("urn:ietf:params:oauth:grant-type:token-exchange", GrantType.TOKEN_EXCHANGE),
        Arguments.of("token_exchange", GrantType.TOKEN_EXCHANGE),
        Arguments.of("TOKEN_EXCHANGE", GrantType.TOKEN_EXCHANGE));
  }

  static Stream<Arguments> initialTestCases() {
    return Stream.of(
        Arguments.of(GrantType.CLIENT_CREDENTIALS, true),
        Arguments.of(GrantType.AUTHORIZATION_CODE, true),
        Arguments.of(GrantType.REFRESH_TOKEN, false),
        Arguments.of(GrantType.TOKEN_EXCHANGE, true));
  }

  static Stream<Arguments> requiresUserInteractionTestCases() {
    return Stream.of(
        Arguments.of(GrantType.CLIENT_CREDENTIALS, false),
        Arguments.of(GrantType.AUTHORIZATION_CODE, true),
        Arguments.of(GrantType.REFRESH_TOKEN, false),
        Arguments.of(GrantType.TOKEN_EXCHANGE, false));
  }
}
