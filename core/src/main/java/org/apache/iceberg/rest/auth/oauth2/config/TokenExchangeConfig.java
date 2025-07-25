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

import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Configuration properties for the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 *
 * <p>This flow allows a client to exchange one token for another, typically to obtain a token that
 * is more suitable for the target resource or service.
 */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface TokenExchangeConfig {

  String GROUP_NAME = "token-exchange";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String SUBJECT_TOKEN = "subject-token";
  String SUBJECT_TOKEN_TYPE = "subject-token-type";
  String ACTOR_TOKEN = "actor-token";
  String ACTOR_TOKEN_TYPE = "actor-token-type";
  String REQUESTED_TOKEN_TYPE = "requested-token-type";
  String RESOURCE = "resource";
  String AUDIENCES = "audiences";

  /**
   * The subject token to exchange.
   *
   * <p>If this value is present, the subject token will be used as-is. If this value is not
   * present, the subject token will be dynamically fetched using the configuration provided under
   * the {@value #SUBJECT_TOKEN} prefix.
   */
  @ConfigOption(SUBJECT_TOKEN)
  @Value.Redacted
  Optional<TypelessAccessToken> subjectToken();

  /**
   * The type of the subject token. Must be a valid URN. The default is {@code
   * urn:ietf:params:oauth:token-type:access_token}.
   *
   * <p>If the OAuth2 client is configured to dynamically fetch the subject token, this property is
   * ignored since only access tokens can be dynamically fetched.
   *
   * @see TokenExchangeConfig#SUBJECT_TOKEN_TYPE
   */
  @ConfigOption(SUBJECT_TOKEN_TYPE)
  @Value.Default
  default TokenTypeURI subjectTokenType() {
    return TokenTypeURI.ACCESS_TOKEN;
  }

  /**
   * The actor token to exchange.
   *
   * <p>If this value is present, the actor token will be used as-is. If this value is not present,
   * the actor token will be dynamically fetched using the configuration provided under the {@value
   * #ACTOR_TOKEN} prefix. If no configuration is provided, no actor token will be used.
   */
  @ConfigOption(ACTOR_TOKEN)
  @Value.Redacted
  Optional<TypelessAccessToken> actorToken();

  /**
   * The type of the actor token. Must be a valid URN. The default is {@code
   * urn:ietf:params:oauth:token-type:access_token}.
   *
   * <p>If the OAuth2 client is configured to dynamically fetch the actor token, this property is
   * ignored since only access tokens can be dynamically fetched.
   *
   * @see TokenExchangeConfig#ACTOR_TOKEN_TYPE
   */
  @ConfigOption(ACTOR_TOKEN_TYPE)
  @Value.Default
  default TokenTypeURI actorTokenType() {
    return TokenTypeURI.ACCESS_TOKEN;
  }

  /**
   * The type of the requested security token. Must be a valid URN. The default is {@code
   * urn:ietf:params:oauth:token-type:access_token}.
   */
  @ConfigOption(REQUESTED_TOKEN_TYPE)
  @Value.Default
  default TokenTypeURI requestedTokenType() {
    return TokenTypeURI.ACCESS_TOKEN;
  }

  /**
   * The configuration to use for fetching the subject token. Required if {@value #SUBJECT_TOKEN} is
   * not set.
   *
   * <p>When this set of properties is provided, a separate OAuth2 client will be created to fetch
   * the subject token and refresh it if necessary.
   *
   * <p>This is a prefix property; any property that can be set under the {@value
   * OAuth2Config#PREFIX} prefix can also be set under this prefix.
   *
   * <p>Example:
   *
   * <pre>{@code
   * rest.auth.oauth2.grant-type=urn:ietf:params:oauth:grant-type:token-exchange
   * rest.auth.oauth2.token-endpoint=https://main-token-endpoint.com/token
   * rest.auth.oauth2.client-id=main-client-id
   * rest.auth.oauth2.client-secret=main-client-secret
   * rest.auth.oauth2.token-exchange.subject-token.grant-type=client_credentials
   * rest.auth.oauth2.token-exchange.subject-token.token-endpoint=https://subject-token-endpoint.com/token
   * rest.auth.oauth2.token-exchange.subject-token.client-id=subject-client-id
   * rest.auth.oauth2.token-exchange.subject-token.client-secret=subject-client-secret
   * }</pre>
   *
   * The above configuration will result in a token exchange where the subject token is obtained
   * using the client credentials grant type, with specific client ID and secret, and a different
   * token endpoint.
   */
  @ConfigOption(value = SUBJECT_TOKEN, prefixMap = true)
  Optional<OAuth2Config> subjectTokenConfig();

  /**
   * The configuration to use for fetching the actor token. Optional; required only if {@value
   * #ACTOR_TOKEN} is not set but an actor token is required.
   *
   * <p>When this set of properties is provided, a separate OAuth2 client will be created to fetch
   * the actor token and refresh it if necessary.
   *
   * <p>This is a prefix property; any property that can be set under the {@value
   * OAuth2Config#PREFIX} prefix can also be set under this prefix.
   *
   * <p>Example:
   *
   * <pre>{@code
   * rest.auth.oauth2.grant-type=urn:ietf:params:oauth:grant-type:token-exchange
   * rest.auth.oauth2.token-endpoint=https://main-token-endpoint.com/token
   * rest.auth.oauth2.client-id=main-client-id
   * rest.auth.oauth2.client-secret=main-client-secret
   * rest.auth.oauth2.token-exchange.actor-token.grant-type=client_credentials
   * rest.auth.oauth2.token-exchange.actor-token.token-endpoint=https://actor-token-endpoint.com/token
   * rest.auth.oauth2.token-exchange.actor-token.client-id=actor-client-id
   * rest.auth.oauth2.token-exchange.actor-token.client-secret=actor-client-secret
   * }</pre>
   *
   * The above configuration will result in a token exchange where the actor token is obtained using
   * the client credentials grant type, with specific client ID and secret, and a different token
   * endpoint.
   */
  @ConfigOption(value = ACTOR_TOKEN, prefixMap = true)
  Optional<OAuth2Config> actorTokenConfig();

  /**
   * A URI that indicates the target service or resource where the client intends to use the
   * requested security token. Optional.
   */
  @ConfigOption(RESOURCE)
  Optional<URI> resource();

  /**
   * The logical name(s) of the target service where the client intends to use the requested
   * security token. This serves a purpose similar to the resource parameter but with the client
   * providing a logical name for the target service.
   *
   * <p>Optional. Can be a single value or a comma-separated list of values.
   */
  @ConfigOption(AUDIENCES)
  List<Audience> audiences();

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (subjectToken().isEmpty()) {
      validator.check(
          subjectTokenType().equals(TokenTypeURI.ACCESS_TOKEN),
          PREFIX + SUBJECT_TOKEN_TYPE,
          "subject token type must be %s when using dynamic subject token",
          TokenTypeURI.ACCESS_TOKEN);
    }

    if (actorToken().isEmpty()) {
      validator.check(
          actorTokenType().equals(TokenTypeURI.ACCESS_TOKEN),
          PREFIX + ACTOR_TOKEN_TYPE,
          "actor token type must be %s when using dynamic actor token",
          TokenTypeURI.ACCESS_TOKEN);
    }

    validator.validate();
  }

  static ImmutableTokenExchangeConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    Map<String, String> subjectTokenProperties =
        ConfigUtils.prefixedMap(
            RESTUtil.extractPrefixMap(props, SUBJECT_TOKEN + '.'), OAuth2Config.PREFIX);
    Map<String, String> actorTokenProperties =
        ConfigUtils.prefixedMap(
            RESTUtil.extractPrefixMap(props, ACTOR_TOKEN + '.'), OAuth2Config.PREFIX);
    return ImmutableTokenExchangeConfig.builder()
        .subjectToken(ConfigUtils.parseOptional(props, SUBJECT_TOKEN, TypelessAccessToken::new))
        .subjectTokenType(
            ConfigUtils.parseOptional(props, SUBJECT_TOKEN_TYPE, TokenTypeURI::parse)
                .orElse(TokenTypeURI.ACCESS_TOKEN))
        .actorToken(ConfigUtils.parseOptional(props, ACTOR_TOKEN, TypelessAccessToken::new))
        .actorTokenType(
            ConfigUtils.parseOptional(props, ACTOR_TOKEN_TYPE, TokenTypeURI::parse)
                .orElse(TokenTypeURI.ACCESS_TOKEN))
        .requestedTokenType(
            ConfigUtils.parseOptional(props, REQUESTED_TOKEN_TYPE, TokenTypeURI::parse)
                .orElse(TokenTypeURI.ACCESS_TOKEN))
        .resource(ConfigUtils.parseOptional(props, RESOURCE, URI::create))
        .audiences(ConfigUtils.parseList(props, AUDIENCES, ",", Audience::new))
        .subjectTokenConfig(
            subjectTokenProperties.isEmpty()
                ? Optional.empty()
                : Optional.of(OAuth2Config.fromProperties(subjectTokenProperties)))
        .actorTokenConfig(
            actorTokenProperties.isEmpty()
                ? Optional.empty()
                : Optional.of(OAuth2Config.fromProperties(actorTokenProperties)));
  }
}
