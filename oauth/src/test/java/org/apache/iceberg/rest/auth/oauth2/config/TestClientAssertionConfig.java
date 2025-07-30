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
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.ALGORITHM;
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.AUDIENCE;
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.EXTRA_CLAIMS_PREFIX;
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.ISSUER;
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.PRIVATE_KEY;
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.SUBJECT;
import static org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion.TOKEN_LIFESPAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.rest.auth.oauth2.auth.JwtSigningAlgorithm;
import org.apache.iceberg.rest.auth.oauth2.config.validator.ConfigValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestClientAssertionConfig {

  @TempDir static Path tempDir;

  static Path tempFile;

  @BeforeAll
  static void createFile() throws IOException {
    tempFile = Files.createTempFile(tempDir, "private-key", ".pem");
  }

  @ParameterizedTest
  @MethodSource
  void testValidate(ClientAssertionConfig.Builder config, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(config::build)
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            ClientAssertionConfig.builder().algorithm(JwtSigningAlgorithm.RSA_SHA256),
            singletonList(
                "client assertion: JWT signing algorithm RS256 requires a private key (rest.auth.oauth2.client-assertion.jwt.algorithm / rest.auth.oauth2.client-assertion.jwt.private-key)")),
        Arguments.of(
            ClientAssertionConfig.builder().privateKey(Paths.get("/invalid/path")),
            singletonList(
                "client assertion: private key path '/invalid/path' is not a file or is not readable (rest.auth.oauth2.client-assertion.jwt.private-key)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(
      Map<String, String> properties, ClientAssertionConfig expected, Throwable expectedThrowable) {
    if (expectedThrowable == null) {
      ClientAssertionConfig actual = ClientAssertionConfig.builder().from(properties).build();
      assertThat(actual).isEqualTo(expected);
    } else {
      Throwable actual = catchThrowable(() -> ClientAssertionConfig.builder().from(properties));
      assertThat(actual)
          .isInstanceOf(expectedThrowable.getClass())
          .hasMessage(expectedThrowable.getMessage());
    }
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(null, null, new NullPointerException("Invalid properties map: null")),
        Arguments.of(
            Map.of(
                ISSUER,
                "https://example.com/token",
                SUBJECT,
                "subject",
                AUDIENCE,
                "audience",
                TOKEN_LIFESPAN,
                "PT1H",
                EXTRA_CLAIMS_PREFIX + "key1",
                "value1",
                ALGORITHM,
                "RS256",
                PRIVATE_KEY,
                tempFile.toString()),
            ClientAssertionConfig.builder()
                .issuer("https://example.com/token")
                .subject("subject")
                .audience("audience")
                .tokenLifespan(Duration.ofHours(1))
                .extraClaims(Map.of("key1", "value1"))
                .algorithm(JwtSigningAlgorithm.RSA_SHA256)
                .privateKey(tempFile)
                .build(),
            null));
  }

  @ParameterizedTest
  @MethodSource
  void testMerge(
      ClientAssertionConfig base, Map<String, String> properties, ClientAssertionConfig expected) {
    ClientAssertionConfig merged = base.merge(properties);
    assertThat(merged).isEqualTo(expected);
  }

  static Stream<Arguments> testMerge() {
    return Stream.of(
        emptyBase(), emptyProperties(), nonEmptyBaseNonEmptyProperties(), baseCleared());
  }

  private static Arguments emptyBase() {
    ClientAssertionConfig base = ClientAssertionConfig.builder().build();
    Map<String, String> properties =
        Map.of(
            ISSUER,
            "https://example.com/token",
            SUBJECT,
            "subject",
            AUDIENCE,
            "audience",
            TOKEN_LIFESPAN,
            "PT1H",
            EXTRA_CLAIMS_PREFIX + "key1",
            "value1",
            ALGORITHM,
            "RS256",
            PRIVATE_KEY,
            tempFile.toString());
    ClientAssertionConfig expected =
        ClientAssertionConfig.builder()
            .issuer("https://example.com/token")
            .subject("subject")
            .audience("audience")
            .tokenLifespan(Duration.ofHours(1))
            .extraClaims(Map.of("key1", "value1"))
            .algorithm(JwtSigningAlgorithm.RSA_SHA256)
            .privateKey(tempFile)
            .build();
    return Arguments.of(base, properties, expected);
  }

  private static Arguments emptyProperties() {
    ClientAssertionConfig base =
        ClientAssertionConfig.builder()
            .issuer("https://example.com/token")
            .subject("subject")
            .audience("audience")
            .tokenLifespan(Duration.ofHours(1))
            .extraClaims(Map.of("key1", "value1"))
            .algorithm(JwtSigningAlgorithm.RSA_SHA256)
            .privateKey(tempFile)
            .build();
    return Arguments.of(base, Map.of(), base);
  }

  private static Arguments nonEmptyBaseNonEmptyProperties() {
    ClientAssertionConfig base =
        ClientAssertionConfig.builder()
            .issuer("https://example.com/token")
            .subject("subject")
            .audience("audience")
            .tokenLifespan(Duration.ofHours(1))
            .extraClaims(Map.of("key1", "value1"))
            .algorithm(JwtSigningAlgorithm.RSA_SHA256)
            .privateKey(tempFile)
            .build();
    Map<String, String> properties =
        Map.of(
            ISSUER,
            "https://example2.com/token",
            SUBJECT,
            "subject2",
            AUDIENCE,
            "audience2",
            TOKEN_LIFESPAN,
            "PT2H",
            EXTRA_CLAIMS_PREFIX + "key2",
            "value2",
            ALGORITHM,
            "RS384",
            PRIVATE_KEY,
            tempFile.toString());
    ClientAssertionConfig expected =
        ClientAssertionConfig.builder()
            .issuer("https://example2.com/token")
            .subject("subject2")
            .audience("audience2")
            .tokenLifespan(Duration.ofHours(2))
            .extraClaims(Map.of("key1", "value1", "key2", "value2"))
            .algorithm(JwtSigningAlgorithm.RSA_SHA384)
            .privateKey(tempFile)
            .build();
    return Arguments.of(base, properties, expected);
  }

  private static Arguments baseCleared() {
    ClientAssertionConfig base =
        ClientAssertionConfig.builder()
            .issuer("https://example.com/token")
            .subject("subject")
            .audience("audience")
            .tokenLifespan(Duration.ofHours(1))
            .extraClaims(Map.of("key1", "value1"))
            .algorithm(JwtSigningAlgorithm.RSA_SHA256)
            .privateKey(tempFile)
            .build();
    Map<String, String> properties =
        Map.of(
            ISSUER,
            "",
            SUBJECT,
            "",
            AUDIENCE,
            "",
            TOKEN_LIFESPAN,
            "",
            EXTRA_CLAIMS_PREFIX + "key1",
            "",
            ALGORITHM,
            "",
            PRIVATE_KEY,
            "");
    ClientAssertionConfig expected = ClientAssertionConfig.DEFAULT;
    return Arguments.of(base, properties, expected);
  }
}
