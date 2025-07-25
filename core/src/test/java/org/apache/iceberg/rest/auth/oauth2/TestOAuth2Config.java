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

import static org.apache.iceberg.rest.auth.oauth2.OAuth2Config.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ResourceOwnerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOAuth2Config {

  @TempDir static Path tempDir;

  static Path tempFile;

  @BeforeAll
  static void createFile() throws IOException {
    tempFile = Files.createTempFile(tempDir, "private-key", ".pem");
  }

  @Test
  void testFromProperties() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(PREFIX + BasicConfig.TOKEN_ENDPOINT, "https://example.com/token")
            .put(PREFIX + BasicConfig.CLIENT_ID, "Client")
            .put(PREFIX + BasicConfig.CLIENT_SECRET, "w00t")
            .put(PREFIX + BasicConfig.SCOPE, "test")
            .build();
    OAuth2Config config = OAuth2Config.fromProperties(properties);
    assertThat(config).isNotNull();
    assertThat(config.basicConfig().tokenEndpoint())
        .contains(URI.create("https://example.com/token"));
    assertThat(config.basicConfig().grantType()).isEqualTo(GrantType.CLIENT_CREDENTIALS);
    assertThat(config.basicConfig().clientId()).contains(new ClientID("Client"));
    assertThat(config.basicConfig().clientSecret()).contains(new Secret("w00t"));
    assertThat(config.basicConfig().scope()).contains(new Scope("test"));
    assertThat(config.basicConfig().extraRequestParameters()).isEmpty();
    assertThat(config.basicConfig().timeout()).isEqualTo(Duration.ofMinutes(5));
  }

  @ParameterizedTest
  @MethodSource
  void testValidate(Map<String, String> properties, List<String> expected) {
    Throwable throwable = catchThrowable(() -> OAuth2Config.fromProperties(properties));
    assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    expected.forEach(e -> assertThat(throwable).hasMessageContaining(e));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.GRANT_TYPE,
                GrantType.PASSWORD.getValue(),
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_SECRET,
                "s3cr3t"),
            List.of(
                "username must be set if grant type is 'password' (rest.auth.oauth2.resource-owner.username)",
                "password must be set if grant type is 'password' (rest.auth.oauth2.resource-owner.password)")),
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.GRANT_TYPE,
                GrantType.PASSWORD.getValue(),
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                ResourceOwnerConfig.PREFIX + ResourceOwnerConfig.USERNAME,
                ""),
            List.of(
                "username must be set if grant type is 'password' (rest.auth.oauth2.resource-owner.username)",
                "password must be set if grant type is 'password' (rest.auth.oauth2.resource-owner.password)")),
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.GRANT_TYPE,
                GrantType.PASSWORD.getValue(),
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                ResourceOwnerConfig.PREFIX + ResourceOwnerConfig.USERNAME,
                "Alice"),
            List.of(
                "password must be set if grant type is 'password' (rest.auth.oauth2.resource-owner.password)")),
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.GRANT_TYPE,
                GrantType.AUTHORIZATION_CODE.getValue(),
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_SECRET,
                "s3cr3t"),
            List.of(
                "either issuer URL or authorization endpoint must be set if grant type is 'authorization_code' (rest.auth.oauth2.issuer-url / rest.auth.oauth2.auth-code.endpoint)")),
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.GRANT_TYPE,
                GrantType.DEVICE_CODE.getValue(),
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_SECRET,
                "s3cr3t"),
            List.of(
                "either issuer URL or device authorization endpoint must be set if grant type is 'urn:ietf:params:oauth:grant-type:device_code' (rest.auth.oauth2.issuer-url / rest.auth.oauth2.device-code.endpoint)")),
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                PREFIX + BasicConfig.CLIENT_AUTH,
                "client_secret_jwt",
                ClientAssertionConfig.PREFIX + ClientAssertionConfig.ALGORITHM,
                "RS256",
                ClientAssertionConfig.PREFIX + ClientAssertionConfig.PRIVATE_KEY,
                tempFile.toString()),
            List.of(
                "client authentication method 'client_secret_jwt' is not compatible with JWS algorithm 'RS256' (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-jwt.algorithm)",
                "client authentication method 'client_secret_jwt' must not have a private key configured (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-jwt.private-key)")),
        Arguments.of(
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                PREFIX + BasicConfig.CLIENT_ID,
                "Client1",
                PREFIX + BasicConfig.CLIENT_AUTH,
                "private_key_jwt",
                ClientAssertionConfig.PREFIX + ClientAssertionConfig.ALGORITHM,
                "HS256"),
            List.of(
                "client authentication method 'private_key_jwt' is not compatible with JWS algorithm 'HS256' (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-jwt.algorithm)",
                "client authentication method 'private_key_jwt' requires a private key (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-jwt.private-key)")));
  }
}
