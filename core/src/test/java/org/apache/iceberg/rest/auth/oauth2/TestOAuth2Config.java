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
package org.apache.iceberg.rest.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOAuth2Config {

  @Test
  void testFromProperties() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(BasicConfig.TOKEN_ENDPOINT, "https://example.com/token")
            .put(BasicConfig.CLIENT_ID, "Client")
            .put(BasicConfig.CLIENT_SECRET, "w00t")
            .put(BasicConfig.GRANT_TYPE, GrantType.TOKEN_EXCHANGE.getValue())
            .put(BasicConfig.SCOPE, "test")
            .put(BasicConfig.EXTRA_PARAMS + ".key1", "value1")
            .put(BasicConfig.EXTRA_PARAMS + ".key2", "value2")
            .put(TokenRefreshConfig.SAFETY_MARGIN, "PT20S")
            .put(TokenExchangeConfig.SUBJECT_TOKEN, "subject-token")
            .build();
    OAuth2Config config = OAuth2Config.of(properties);
    assertThat(config).isNotNull();
    assertThat(config.basicConfig()).isNotNull();
    assertThat(config.basicConfig().tokenEndpoint())
        .contains(URI.create("https://example.com/token"));
    assertThat(config.basicConfig().grantType()).isEqualTo(GrantType.TOKEN_EXCHANGE);
    assertThat(config.basicConfig().clientId()).contains(new ClientID("Client"));
    assertThat(config.basicConfig().clientSecret()).contains(new Secret("w00t"));
    assertThat(config.basicConfig().scope()).contains(new Scope("test"));
    assertThat(config.basicConfig().extraRequestParameters())
        .isEqualTo(Map.of("key1", "value1", "key2", "value2"));
    assertThat(config.tokenRefreshConfig()).isNotNull();
    assertThat(config.tokenRefreshConfig().safetyMargin()).isEqualTo(Duration.ofSeconds(20));
    assertThat(config.tokenExchangeConfig()).isNotNull();
    assertThat(config.tokenExchangeConfig().subjectTokenString()).contains("subject-token");
  }

  @ParameterizedTest
  @MethodSource
  void testValidate(Map<String, String> properties, List<String> expected) {
    Throwable throwable = catchThrowable(() -> OAuth2Config.of(properties));
    assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    expected.forEach(e -> assertThat(throwable).hasMessageContaining(e));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(
                BasicConfig.GRANT_TYPE,
                GrantType.TOKEN_EXCHANGE.getValue(),
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t"),
            List.of(
                "subject token must be set if grant type is 'urn:ietf:params:oauth:grant-type:token-exchange' (rest.auth.oauth2.token-exchange.subject-token)")));
  }
}
