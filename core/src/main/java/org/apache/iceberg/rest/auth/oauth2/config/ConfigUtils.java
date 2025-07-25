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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

public final class ConfigUtils {

  public static final List<GrantType> SUPPORTED_GRANT_TYPES =
      List.of(
          GrantType.CLIENT_CREDENTIALS,
          GrantType.PASSWORD,
          GrantType.AUTHORIZATION_CODE,
          GrantType.DEVICE_CODE,
          GrantType.TOKEN_EXCHANGE,
          GrantType.REFRESH_TOKEN);

  public static final List<GrantType> SUPPORTED_INITIAL_GRANT_TYPES =
      List.of(
          GrantType.CLIENT_CREDENTIALS,
          GrantType.PASSWORD,
          GrantType.AUTHORIZATION_CODE,
          GrantType.DEVICE_CODE,
          GrantType.TOKEN_EXCHANGE);

  public static final List<GrantType> SUPPORTED_REFRESH_GRANT_TYPES =
      List.of(GrantType.REFRESH_TOKEN, GrantType.TOKEN_EXCHANGE);

  public static final List<ClientAuthenticationMethod> SUPPORTED_CLIENT_AUTH_METHODS =
      List.of(
          ClientAuthenticationMethod.NONE,
          ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
          ClientAuthenticationMethod.CLIENT_SECRET_POST,
          ClientAuthenticationMethod.CLIENT_SECRET_JWT,
          ClientAuthenticationMethod.PRIVATE_KEY_JWT);

  public static final List<CodeChallengeMethod> SUPPORTED_CODE_CHALLENGE_METHODS =
      List.of(CodeChallengeMethod.PLAIN, CodeChallengeMethod.S256);

  public static boolean requiresClientSecret(ClientAuthenticationMethod method) {
    return method.equals(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
        || method.equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)
        || method.equals(ClientAuthenticationMethod.CLIENT_SECRET_JWT);
  }

  public static boolean requiresJwsAlgorithm(ClientAuthenticationMethod method) {
    return method.equals(ClientAuthenticationMethod.PRIVATE_KEY_JWT)
        || method.equals(ClientAuthenticationMethod.CLIENT_SECRET_JWT);
  }

  public static boolean requiresUserInteraction(GrantType grantType) {
    return grantType.equals(GrantType.AUTHORIZATION_CODE)
        || grantType.equals(GrantType.DEVICE_CODE);
  }

  public static Map<String, String> prefixedMap(Map<String, String> properties, String prefix) {
    return properties.entrySet().stream()
        .map(e -> Map.entry(prefix + e.getKey(), e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static Optional<String> parseOptional(Map<String, String> properties, String option) {
    return parseOptional(properties, option, s -> s);
  }

  public static <T> Optional<T> parseOptional(
      Map<String, String> properties, String option, ConfigParser<T> parser) {
    return Optional.ofNullable(properties.get(option)).map(parser.asFunction());
  }

  public static OptionalInt parseOptionalInt(Map<String, String> properties, String option) {
    return Optional.ofNullable(properties.get(option))
        .map(Integer::parseInt)
        .map(OptionalInt::of)
        .orElseGet(OptionalInt::empty);
  }

  public static List<String> parseList(
      Map<String, String> properties, String option, String delimiter) {
    return parseList(properties, option, delimiter, s -> s);
  }

  public static <T> List<T> parseList(
      Map<String, String> properties, String option, String delimiter, ConfigParser<T> parser) {
    return Optional.ofNullable(properties.get(option))
        .map(s -> Splitter.on(delimiter).trimResults().omitEmptyStrings().splitToStream(s))
        .orElseGet(Stream::empty)
        .map(parser.asFunction())
        .collect(Collectors.toList());
  }

  @FunctionalInterface
  public interface ConfigParser<T> {

    default Function<String, T> asFunction() {
      return s -> {
        try {
          return parse(s);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }

    T parse(String value) throws Exception;
  }

  private ConfigUtils() {}
}
