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
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.CLIENT_TYPE;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.COMPRESSION_ENABLED;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.CONNECT_TIMEOUT;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.HEADERS;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.PREFIX;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.PROXY_HOST;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.PROXY_PASSWORD;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.PROXY_PORT;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.PROXY_USERNAME;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.READ_TIMEOUT;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.SSL_CIPHER_SUITES;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.SSL_HOSTNAME_VERIFICATION_ENABLED;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.SSL_PROTOCOLS;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.SSL_TRUSTSTORE_PATH;
import static org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig.SSL_TRUST_ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Stream;
import org.apache.iceberg.rest.auth.oauth2.http.HttpClientType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestHttpClientConfig {

  static Path tempFile;

  @BeforeAll
  static void createFile(@TempDir Path tempDir) throws IOException {
    tempFile = Files.createTempFile(tempDir, "private-key", ".pem");
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @ParameterizedTest
  @MethodSource
  void testValidate(Map<String, String> properties, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> HttpClientConfig.fromProperties(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(PREFIX + SSL_TRUSTSTORE_PATH, "/invalid/path"),
            singletonList(
                "http: SSL truststore path '/invalid/path' is not a file or is not readable (rest.auth.oauth2.http.ssl.trust-store.path)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(Map<String, String> properties, HttpClientConfig expected) {
    HttpClientConfig actual = HttpClientConfig.fromProperties(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableHttpClientConfig.builder().build()),
        Arguments.of(
            Map.of(PREFIX + CLIENT_TYPE, "apache"),
            ImmutableHttpClientConfig.builder().clientType(HttpClientType.APACHE).build()),
        Arguments.of(
            Map.of(PREFIX + READ_TIMEOUT, "PT1M"),
            ImmutableHttpClientConfig.builder().readTimeout(Duration.ofMinutes(1)).build()),
        Arguments.of(
            Map.of(PREFIX + CONNECT_TIMEOUT, "PT30S"),
            ImmutableHttpClientConfig.builder().connectionTimeout(Duration.ofSeconds(30)).build()),
        Arguments.of(
            Map.of(
                PREFIX + HEADERS + ".X-Forwarded-For", "1.2.3.4",
                PREFIX + HEADERS + ".X-Custom-Header", "value"),
            ImmutableHttpClientConfig.builder()
                .putHeaders("X-Forwarded-For", "1.2.3.4")
                .putHeaders("X-Custom-Header", "value")
                .build()),
        Arguments.of(
            Map.of(PREFIX + COMPRESSION_ENABLED, "false"),
            ImmutableHttpClientConfig.builder().compressionEnabled(false).build()),
        Arguments.of(
            Map.of(PREFIX + SSL_PROTOCOLS, "TLSv1.2"),
            ImmutableHttpClientConfig.builder().addSslProtocols("TLSv1.2").build()),
        Arguments.of(
            Map.of(PREFIX + SSL_PROTOCOLS, "TLSv1.2,TLSv1.3"),
            ImmutableHttpClientConfig.builder().addSslProtocols("TLSv1.2", "TLSv1.3").build()),
        Arguments.of(
            Map.of(PREFIX + SSL_CIPHER_SUITES, "TLS_RSA_WITH_AES_128_CBC_SHA"),
            ImmutableHttpClientConfig.builder()
                .addSslCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                .build()),
        Arguments.of(
            Map.of(
                PREFIX + SSL_CIPHER_SUITES,
                "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA"),
            ImmutableHttpClientConfig.builder()
                .addSslCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                .addSslCipherSuites("TLS_RSA_WITH_AES_256_CBC_SHA")
                .build()),
        Arguments.of(
            Map.of(PREFIX + SSL_HOSTNAME_VERIFICATION_ENABLED, "false"),
            ImmutableHttpClientConfig.builder().sslHostnameVerificationEnabled(false).build()),
        Arguments.of(
            Map.of(PREFIX + SSL_TRUST_ALL, "true"),
            ImmutableHttpClientConfig.builder().sslTrustAll(true).build()),
        Arguments.of(
            Map.of(PREFIX + SSL_TRUSTSTORE_PATH, tempFile.toString()),
            ImmutableHttpClientConfig.builder()
                .sslTrustStorePath(Paths.get(tempFile.toString()))
                .build()),
        Arguments.of(
            Map.of(PREFIX + SSL_TRUSTSTORE_PASSWORD, "truststore-pass"),
            ImmutableHttpClientConfig.builder().sslTrustStorePassword("truststore-pass").build()),
        Arguments.of(
            Map.of(PREFIX + PROXY_HOST, "proxy.example.com"),
            ImmutableHttpClientConfig.builder().proxyHost("proxy.example.com").build()),
        Arguments.of(
            Map.of(PREFIX + PROXY_PORT, "8080"),
            ImmutableHttpClientConfig.builder().proxyPort(OptionalInt.of(8080)).build()),
        Arguments.of(
            Map.of(PREFIX + PROXY_USERNAME, "proxy-user"),
            ImmutableHttpClientConfig.builder().proxyUsername("proxy-user").build()),
        Arguments.of(
            Map.of(PREFIX + PROXY_PASSWORD, "proxy-pass"),
            ImmutableHttpClientConfig.builder().proxyPassword("proxy-pass").build()));
  }
}
