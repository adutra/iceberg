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

import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.ALGORITHM;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.AUDIENCES;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.EXTRA_CLAIMS;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.ISSUER;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.KEY_ID;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.PREFIX;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.PRIVATE_KEY;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.SUBJECT;
import static org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig.TOKEN_LIFESPAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.Subject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestClientAssertionConfig {

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
        .isThrownBy(() -> ClientAssertionConfig.fromProperties(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(PREFIX + ClientAssertionConfig.ALGORITHM, "RS256"),
            List.of(
                "client assertion: JWS signing algorithm 'RS256' requires a private key "
                    + "(rest.auth.oauth2.client-jwt.algorithm / rest.auth.oauth2.client-jwt.private-key)")),
        Arguments.of(
            Map.of(
                PREFIX + ClientAssertionConfig.ALGORITHM,
                "HS256",
                PREFIX + ClientAssertionConfig.PRIVATE_KEY,
                tempFile.toString()),
            List.of(
                "client assertion: private key must not be set for JWS algorithm 'HS256' "
                    + "(rest.auth.oauth2.client-jwt.algorithm / rest.auth.oauth2.client-jwt.private-key)")),
        Arguments.of(
            Map.of(
                PREFIX + ClientAssertionConfig.ALGORITHM,
                "RSA_SHA256",
                PREFIX + ClientAssertionConfig.PRIVATE_KEY,
                tempFile.toString()),
            List.of(
                "client assertion: unsupported JWS algorithm 'RSA_SHA256', must be one of: "
                    + "'HS256', 'HS384', 'HS512', 'RS256', 'RS384', 'RS512', 'PS256', 'PS384', 'PS512', 'ES256', 'ES256K', 'ES384', 'ES512', 'EdDSA', 'Ed25519', 'Ed448' "
                    + "(rest.auth.oauth2.client-jwt.algorithm)")),
        Arguments.of(
            Map.of(PREFIX + ClientAssertionConfig.PRIVATE_KEY, "/invalid/path"),
            List.of(
                "client assertion: private key path '/invalid/path' is not a file or is not readable "
                    + "(rest.auth.oauth2.client-jwt.private-key)")));
  }

  @Test
  void testKeyIdOptional() {
    Map<String, String> properties =
        Map.of(
            PREFIX + ClientAssertionConfig.ALGORITHM,
            "RS256",
            PREFIX + ClientAssertionConfig.PRIVATE_KEY,
            tempFile.toString());
    ClientAssertionConfig config = ClientAssertionConfig.fromProperties(properties).build();
    assertThat(config.keyId()).isEmpty();
  }

  @Test
  void testKeyIdPresent() {
    Map<String, String> properties =
        Map.of(
            PREFIX + ClientAssertionConfig.ALGORITHM, "RS256",
            PREFIX + ClientAssertionConfig.PRIVATE_KEY, tempFile.toString(),
            PREFIX + ClientAssertionConfig.KEY_ID, "my-key-123");
    ClientAssertionConfig config = ClientAssertionConfig.fromProperties(properties).build();
    assertThat(config.keyId()).hasValue("my-key-123");
  }

  @Test
  void testAudienceSingleValue() {
    Map<String, String> properties =
        Map.of(PREFIX + ClientAssertionConfig.AUDIENCES, "https://example.com");
    ClientAssertionConfig config = ClientAssertionConfig.fromProperties(properties).build();
    assertThat(config.audiences()).containsExactly(new Audience("https://example.com"));
  }

  @Test
  void testAudienceMultipleValues() {
    Map<String, String> properties =
        Map.of(
            PREFIX + ClientAssertionConfig.AUDIENCES,
            "https://auth1.example.com,https://auth2.example.com");
    ClientAssertionConfig config = ClientAssertionConfig.fromProperties(properties).build();
    assertThat(config.audiences())
        .containsExactly(
            new Audience("https://auth1.example.com"), new Audience("https://auth2.example.com"));
  }

  @ParameterizedTest
  @MethodSource
  void testFromProperties(Map<String, String> properties, ClientAssertionConfig expected) {
    ClientAssertionConfig actual = ClientAssertionConfig.fromProperties(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableClientAssertionConfig.builder().build()),
        Arguments.of(
            Map.of(PREFIX + ISSUER, "https://issuer.example.com"),
            ImmutableClientAssertionConfig.builder()
                .issuer(new Issuer("https://issuer.example.com"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + SUBJECT, "my-subject"),
            ImmutableClientAssertionConfig.builder().subject(new Subject("my-subject")).build()),
        Arguments.of(
            Map.of(PREFIX + AUDIENCES, "https://example.com"),
            ImmutableClientAssertionConfig.builder()
                .addAudiences(new Audience("https://example.com"))
                .build()),
        Arguments.of(
            Map.of(PREFIX + TOKEN_LIFESPAN, "PT10M"),
            ImmutableClientAssertionConfig.builder().tokenLifespan(Duration.ofMinutes(10)).build()),
        Arguments.of(
            Map.of(PREFIX + ALGORITHM, "HS256"),
            ImmutableClientAssertionConfig.builder().algorithm(JWSAlgorithm.HS256).build()),
        Arguments.of(
            Map.of(PREFIX + ALGORITHM, "RS256", PREFIX + PRIVATE_KEY, tempFile.toString()),
            ImmutableClientAssertionConfig.builder()
                .algorithm(JWSAlgorithm.RS256)
                .privateKey(Paths.get(tempFile.toString()))
                .build()),
        Arguments.of(
            Map.of(PREFIX + KEY_ID, "my-key-id"),
            ImmutableClientAssertionConfig.builder().keyId("my-key-id").build()),
        Arguments.of(
            Map.of(
                PREFIX + EXTRA_CLAIMS + ".claim1", "value1",
                PREFIX + EXTRA_CLAIMS + ".claim2", "value2"),
            ImmutableClientAssertionConfig.builder()
                .putExtraClaims("claim1", "value1")
                .putExtraClaims("claim2", "value2")
                .build()));
  }
}
