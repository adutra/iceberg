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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Properties.ClientAssertion;
import org.apache.iceberg.rest.auth.oauth2.auth.JwtSigningAlgorithm;
import org.apache.iceberg.rest.auth.oauth2.config.option.ConfigOption;
import org.apache.iceberg.rest.auth.oauth2.config.option.ConfigOptions;
import org.apache.iceberg.rest.auth.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public interface ClientAssertionConfig {

  Duration DEFAULT_TOKEN_LIFESPAN = Duration.ofMinutes(5);

  ClientAssertionConfig DEFAULT = builder().build();

  /**
   * The issuer of the client assertion JWT. Optional. The default is the client ID.
   *
   * @see ClientAssertion#ISSUER
   */
  Optional<String> issuer();

  /**
   * The subject of the client assertion JWT. Optional. The default is the client ID.
   *
   * @see ClientAssertion#SUBJECT
   */
  Optional<String> subject();

  /**
   * The audience of the client assertion JWT. Optional. The default is the token endpoint.
   *
   * @see ClientAssertion#AUDIENCE
   */
  Optional<String> audience();

  /**
   * The lifespan of the client assertion JWT. Optional. The default is 5 minutes.
   *
   * @see ClientAssertion#TOKEN_LIFESPAN
   */
  @Value.Default
  default Duration tokenLifespan() {
    return DEFAULT_TOKEN_LIFESPAN;
  }

  /**
   * Extra claims to include in the client assertion JWT. Optional. The default is empty.
   *
   * @see ClientAssertion#EXTRA_CLAIMS_PREFIX
   */
  Map<String, String> extraClaims();

  /**
   * The signing algorithm to use for the client assertion JWT. Optional. The default is "HS256" if
   * the authentication method is "client_secret_jwt", or "RS256" if the authentication method is
   * "private_key_jwt".
   *
   * <p>Algorithm names must match either the JWS name or the JCA name of the algorithm.
   *
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc7518#section-3.1">RFC 7518 Section
   *     3.1</a>
   * @see ClientAssertion#ALGORITHM
   */
  Optional<JwtSigningAlgorithm> algorithm();

  /**
   * The path on the local filesystem to the private key to use for signing the client assertion
   * JWT. Required if the authentication method is "private_key_jwt". The file must be in PEM
   * format; it may contain a private key, or a private key and a certificate chain. Only the
   * private key is used.
   *
   * @see ClientAssertion#PRIVATE_KEY
   */
  Optional<Path> privateKey();

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (algorithm().isPresent()) {
      if (algorithm().get().isRsaAlgorithm()) {
        validator.check(
            privateKey().isPresent(),
            List.of(ClientAssertion.ALGORITHM, ClientAssertion.PRIVATE_KEY),
            "client assertion: JWT signing algorithm %s requires a private key",
            algorithm().get().getJwsName());
      }
    }

    if (privateKey().isPresent()) {
      validator.check(
          Files.isReadable(privateKey().get()),
          ClientAssertion.PRIVATE_KEY,
          "client assertion: private key path '%s' is not a file or is not readable",
          privateKey().get());
    }

    validator.validate();
  }

  /** Merges the given properties into this {@link ClientAssertionConfig} and returns the result. */
  default ClientAssertionConfig merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Builder builder = builder();
    builder.issuerOption().set(properties, issuer());
    builder.subjectOption().set(properties, subject());
    builder.audienceOption().set(properties, audience());
    builder.tokenLifespanOption().set(properties, tokenLifespan());
    builder.extraClaimsOption().set(properties, extraClaims());
    builder.algorithmOption().set(properties, algorithm());
    builder.privateKeyOption().set(properties, privateKey());
    return builder.build();
  }

  static Builder builder() {
    return ImmutableClientAssertionConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(ClientAssertionConfig config);

    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      issuerOption().set(properties);
      subjectOption().set(properties);
      audienceOption().set(properties);
      tokenLifespanOption().set(properties);
      extraClaimsOption().set(properties);
      algorithmOption().set(properties);
      privateKeyOption().set(properties);
      return this;
    }

    @CanIgnoreReturnValue
    Builder issuer(String issuer);

    @CanIgnoreReturnValue
    Builder subject(String subject);

    @CanIgnoreReturnValue
    Builder audience(String audience);

    @CanIgnoreReturnValue
    Builder tokenLifespan(Duration tokenLifespan);

    @CanIgnoreReturnValue
    Builder extraClaims(Map<String, ? extends String> extraClaims);

    @CanIgnoreReturnValue
    Builder algorithm(JwtSigningAlgorithm algorithm);

    @CanIgnoreReturnValue
    Builder privateKey(Path privateKey);

    ClientAssertionConfig build();

    default ConfigOption<String> issuerOption() {
      return ConfigOptions.simple(ClientAssertion.ISSUER, this::issuer);
    }

    default ConfigOption<String> subjectOption() {
      return ConfigOptions.simple(ClientAssertion.SUBJECT, this::subject);
    }

    default ConfigOption<String> audienceOption() {
      return ConfigOptions.simple(ClientAssertion.AUDIENCE, this::audience);
    }

    default ConfigOption<Duration> tokenLifespanOption() {
      return ConfigOptions.simple(
          ClientAssertion.TOKEN_LIFESPAN, this::tokenLifespan, Duration::parse);
    }

    default ConfigOption<Map<String, String>> extraClaimsOption() {
      return ConfigOptions.prefixMap(ClientAssertion.EXTRA_CLAIMS_PREFIX, this::extraClaims);
    }

    default ConfigOption<JwtSigningAlgorithm> algorithmOption() {
      return ConfigOptions.simple(
          ClientAssertion.ALGORITHM, this::algorithm, JwtSigningAlgorithm::fromConfigName);
    }

    default ConfigOption<Path> privateKeyOption() {
      return ConfigOptions.simple(ClientAssertion.PRIVATE_KEY, this::privateKey, Paths::get);
    }
  }
}
