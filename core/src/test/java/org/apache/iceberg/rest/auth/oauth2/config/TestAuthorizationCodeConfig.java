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
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.CALLBACK_BIND_HOST;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.CALLBACK_BIND_PORT;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.CALLBACK_CONTEXT_PATH;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.CALLBACK_HTTPS;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.ENDPOINT;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.PKCE_ENABLED;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.PKCE_METHOD;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.PREFIX;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.REDIRECT_URI;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.SSL_CIPHER_SUITES;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.SSL_KEYSTORE_ALIAS;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.SSL_KEYSTORE_PASSWORD;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.SSL_KEYSTORE_PATH;
import static org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig.SSL_PROTOCOLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestAuthorizationCodeConfig {

  static Path tempFile;

  @BeforeAll
  static void createFile(@TempDir Path tempDir) throws IOException {
    tempFile = Files.createTempFile(tempDir, "private-key", ".pem");
  }

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testValidate(Map<String, String> properties, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AuthorizationCodeConfig.fromProperties(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "/auth"),
            singletonList(
                "authorization code flow: authorization endpoint must not be relative (rest.auth.oauth2.auth-code.endpoint)")),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com?query"),
            singletonList(
                "authorization code flow: authorization endpoint must not have a query part (rest.auth.oauth2.auth-code.endpoint)")),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com#fragment"),
            singletonList(
                "authorization code flow: authorization endpoint must not have a fragment part (rest.auth.oauth2.auth-code.endpoint)")),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com", PREFIX + CALLBACK_BIND_PORT, "-1"),
            singletonList(
                "authorization code flow: callback bind port must be between 0 and 65535 (inclusive) (rest.auth.oauth2.auth-code.callback.bind-port)")),
        Arguments.of(
            Map.of(PREFIX + PKCE_METHOD, "PLAIN"),
            singletonList(
                "authorization code flow: code challenge method must be one of: 'plain', 'S256' (rest.auth.oauth2.auth-code.pkce.method)")),
        Arguments.of(
            Map.of(PREFIX + SSL_KEYSTORE_PATH, "/invalid/path"),
            singletonList(
                "authorization code flow: SSL keystore path '/invalid/path' is not a file or is not readable (rest.auth.oauth2.auth-code.ssl.key-store.path)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(Map<String, String> properties, AuthorizationCodeConfig expected) {
    AuthorizationCodeConfig actual = AuthorizationCodeConfig.fromProperties(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableAuthorizationCodeConfig.builder().build()),
        Arguments.of(
            Map.of(PREFIX + ENDPOINT, "https://example.com/auth"),
            ImmutableAuthorizationCodeConfig.builder()
                .authorizationEndpoint(URI.create("https://example.com/auth"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + REDIRECT_URI, "https://example.com/callback"),
            ImmutableAuthorizationCodeConfig.builder()
                .redirectUri(URI.create("https://example.com/callback"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + CALLBACK_HTTPS, "true"),
            ImmutableAuthorizationCodeConfig.builder().callbackHttps(true).build()),
        Arguments.of(
            Map.of(PREFIX + CALLBACK_BIND_HOST, "localhost"),
            ImmutableAuthorizationCodeConfig.builder().callbackBindHost("localhost").build()),
        Arguments.of(
            Map.of(PREFIX + CALLBACK_BIND_PORT, "8080"),
            ImmutableAuthorizationCodeConfig.builder().callbackBindPort(8080).build()),
        Arguments.of(
            Map.of(PREFIX + CALLBACK_CONTEXT_PATH, "/oauth2/callback"),
            ImmutableAuthorizationCodeConfig.builder()
                .callbackContextPath("/oauth2/callback")
                .build()),
        Arguments.of(
            Map.of(PREFIX + PKCE_ENABLED, "false"),
            ImmutableAuthorizationCodeConfig.builder().pkceEnabled(false).build()),
        Arguments.of(
            Map.of(PREFIX + PKCE_METHOD, "plain"),
            ImmutableAuthorizationCodeConfig.builder()
                .codeChallengeMethod(CodeChallengeMethod.PLAIN)
                .build()),
        Arguments.of(
            Map.of(PREFIX + SSL_KEYSTORE_PATH, tempFile.toString()),
            ImmutableAuthorizationCodeConfig.builder()
                .sslKeyStorePath(Paths.get(tempFile.toString()))
                .build()),
        Arguments.of(
            Map.of(PREFIX + SSL_KEYSTORE_PASSWORD, "keystore-pass"),
            ImmutableAuthorizationCodeConfig.builder()
                .sslKeyStorePassword("keystore-pass")
                .build()),
        Arguments.of(
            Map.of(PREFIX + SSL_KEYSTORE_ALIAS, "my-alias"),
            ImmutableAuthorizationCodeConfig.builder().sslKeyStoreAlias("my-alias").build()),
        Arguments.of(
            Map.of(PREFIX + SSL_PROTOCOLS, "TLSv1.2,TLSv1.3"),
            ImmutableAuthorizationCodeConfig.builder()
                .addSslProtocols("TLSv1.2", "TLSv1.3")
                .build()),
        Arguments.of(
            Map.of(
                PREFIX + SSL_CIPHER_SUITES,
                "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA"),
            ImmutableAuthorizationCodeConfig.builder()
                .addSslCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
                .build()));
  }
}
