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
package org.apache.iceberg.rest.oauth2.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestClientAuthentication {

  @ParameterizedTest
  @MethodSource("configNameTestCases")
  void testFromConfigName(String configName, ClientAuthentication expectedClientAuthentication) {
    assertThat(ClientAuthentication.fromConfigName(configName))
        .isEqualTo(expectedClientAuthentication);
  }

  @Test
  void testFromConfigNameInvalidName() {
    assertThatThrownBy(() -> ClientAuthentication.fromConfigName(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid OAuth2 client authentication method: null");
    assertThatThrownBy(() -> ClientAuthentication.fromConfigName(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown OAuth2 client authentication method: ");
    assertThatThrownBy(() -> ClientAuthentication.fromConfigName("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown OAuth2 client authentication method: invalid");
  }

  static Stream<Arguments> configNameTestCases() {
    return Stream.of(
        // lowercase
        Arguments.of("none", ClientAuthentication.NONE),
        Arguments.of("client_secret_basic", ClientAuthentication.CLIENT_SECRET_BASIC),
        Arguments.of("client_secret_post", ClientAuthentication.CLIENT_SECRET_POST),
        // mixed case
        Arguments.of("None", ClientAuthentication.NONE),
        Arguments.of("Client_Secret_Basic", ClientAuthentication.CLIENT_SECRET_BASIC),
        Arguments.of("Client_Secret_Post", ClientAuthentication.CLIENT_SECRET_POST),
        // uppercase
        Arguments.of("NONE", ClientAuthentication.NONE),
        Arguments.of("CLIENT_SECRET_BASIC", ClientAuthentication.CLIENT_SECRET_BASIC),
        Arguments.of("CLIENT_SECRET_POST", ClientAuthentication.CLIENT_SECRET_POST));
  }

  @ParameterizedTest
  @MethodSource("isClientSecretTestCases")
  void testIsClientSecret(
      ClientAuthentication clientAuthentication, boolean expectedRequiresUserInteraction) {
    assertThat(clientAuthentication.isClientSecret()).isEqualTo(expectedRequiresUserInteraction);
  }

  static Stream<Arguments> isClientSecretTestCases() {
    return Stream.of(
        Arguments.of(ClientAuthentication.NONE, false),
        Arguments.of(ClientAuthentication.CLIENT_SECRET_BASIC, true),
        Arguments.of(ClientAuthentication.CLIENT_SECRET_POST, true));
  }
}
