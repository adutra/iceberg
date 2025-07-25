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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.Subject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Configuration properties for JWT client assertion as specified in <a
 * href="https://datatracker.ietf.org/doc/html/rfc7523">JSON Web Token (JWT) Profile for OAuth 2.0
 * Client Authentication and Authorization Grants</a>.
 *
 * <p>These properties allow the client to authenticate using the {@code client_secret_jwt} or
 * {@code private_key_jwt} authentication methods.
 */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface ClientAssertionConfig {

  String GROUP_NAME = "client-jwt";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String ISSUER = "issuer";
  String SUBJECT = "subject";
  String AUDIENCES = "audiences";
  String TOKEN_LIFESPAN = "token-lifespan";
  String ALGORITHM = "algorithm";
  String PRIVATE_KEY = "private-key";
  String KEY_ID = "key-id";
  String EXTRA_CLAIMS = "extra-claims";

  Duration DEFAULT_TOKEN_LIFESPAN = Duration.ofMinutes(5);

  /** The issuer of the client assertion JWT. Optional. The default is the client ID. */
  @ConfigOption(ISSUER)
  Optional<Issuer> issuer();

  /** The subject of the client assertion JWT. Optional. The default is the client ID. */
  @ConfigOption(SUBJECT)
  Optional<Subject> subject();

  /**
   * The audiences(s) of the client assertion JWT. Optional. The default is the token endpoint. Can
   * be a single audiences or a comma-separated list of audiences.
   */
  @ConfigOption(AUDIENCES)
  List<Audience> audiences();

  /**
   * The expiration time of the client assertion JWT. Optional. The default is {@link
   * #DEFAULT_TOKEN_LIFESPAN}.
   */
  @ConfigOption(TOKEN_LIFESPAN)
  @Value.Default
  default Duration tokenLifespan() {
    return DEFAULT_TOKEN_LIFESPAN;
  }

  /**
   * The signing algorithm to use for the client assertion JWT. Optional. The default is {@link
   * JWSAlgorithm#HS512} if the authentication method is {@link
   * ClientAuthenticationMethod#CLIENT_SECRET_JWT}, or {@link JWSAlgorithm#RS512} if the
   * authentication method is {@link ClientAuthenticationMethod#PRIVATE_KEY_JWT}.
   *
   * <p>Algorithm names must match the "alg" Param Value as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc7518#section-3.1">RFC 7518 Section 3.1</a>.
   */
  @ConfigOption(ALGORITHM)
  Optional<JWSAlgorithm> algorithm();

  /**
   * The path on the local filesystem to the private key to use for signing the client assertion
   * JWT. Required if the authentication method is {@link
   * ClientAuthenticationMethod#PRIVATE_KEY_JWT}.
   *
   * <p>The file must be in PEM format; it may contain a private key, or a private key and a
   * certificate chain. Only the private key is used.
   *
   * <p>Supported key formats are:
   *
   * <ul>
   *   <li>RSA PKCS#8 ({@code BEGIN PRIVATE KEY}): always supported
   *   <li>RSA PKCS#1 ({@code BEGIN RSA PRIVATE KEY}): requires the BouncyCastle library
   *   <li>ECDSA ({@code BEGIN EC PRIVATE KEY}): requires the BouncyCastle library
   * </ul>
   *
   * Only unencrypted keys are supported currently.
   */
  @ConfigOption(PRIVATE_KEY)
  @Value.Redacted
  Optional<Path> privateKey();

  /**
   * The key ID (kid) to include in the JWT header. Optional.
   *
   * <p>If specified, this will be included in the "kid" header parameter of the JWT assertion. This
   * is useful when the authorization server needs to identify which key to use for verification
   * from a set of keys.
   *
   * <p>This setting is only supported when using the {@code private_key_jwt} authentication method.
   * It is ignored when using {@code client_secret_jwt}.
   */
  @ConfigOption(KEY_ID)
  Optional<String> keyId();

  /**
   * Extra claims to include in the client assertion JWT. This is a prefix property, and multiple
   * values can be set, each with a different key and value.
   */
  @ConfigOption(value = EXTRA_CLAIMS, prefixMap = true)
  Map<String, String> extraClaims();

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (algorithm().isPresent()) {
      if (JWSAlgorithm.Family.SIGNATURE.contains(algorithm().get())) {
        validator.check(
            privateKey().isPresent(),
            List.of(PREFIX + ALGORITHM, PREFIX + PRIVATE_KEY),
            "client assertion: JWS signing algorithm '%s' requires a private key",
            algorithm().get().getName());
      } else if (JWSAlgorithm.Family.HMAC_SHA.contains(algorithm().get())) {
        validator.check(
            privateKey().isEmpty(),
            List.of(PREFIX + ALGORITHM, PREFIX + PRIVATE_KEY),
            "client assertion: private key must not be set for JWS algorithm '%s'",
            algorithm().get().getName());
      } else {
        validator.check(
            false,
            PREFIX + ALGORITHM,
            "client assertion: unsupported JWS algorithm '%s', must be one of: %s",
            algorithm().get().getName(),
            Stream.concat(
                    JWSAlgorithm.Family.HMAC_SHA.stream(), JWSAlgorithm.Family.SIGNATURE.stream())
                .map(JWSAlgorithm::getName)
                .collect(Collectors.joining("', '", "'", "'")));
      }
    }

    if (privateKey().isPresent()) {
      validator.check(
          Files.isReadable(privateKey().get()),
          PREFIX + PRIVATE_KEY,
          "client assertion: private key path '%s' is not a file or is not readable",
          privateKey().get());
    }

    validator.validate();
  }

  static ImmutableClientAssertionConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    return ImmutableClientAssertionConfig.builder()
        .issuer(ConfigUtils.parseOptional(props, ISSUER, Issuer::new))
        .subject(ConfigUtils.parseOptional(props, SUBJECT, Subject::new))
        .audiences(ConfigUtils.parseList(props, AUDIENCES, ",", Audience::new))
        .tokenLifespan(
            ConfigUtils.parseOptional(props, TOKEN_LIFESPAN, Duration::parse)
                .orElse(DEFAULT_TOKEN_LIFESPAN))
        .algorithm(ConfigUtils.parseOptional(props, ALGORITHM, JWSAlgorithm::parse))
        .privateKey(ConfigUtils.parseOptional(props, PRIVATE_KEY, Paths::get))
        .keyId(ConfigUtils.parseOptional(props, KEY_ID))
        .extraClaims(RESTUtil.extractPrefixMap(props, EXTRA_CLAIMS + '.'));
  }
}
