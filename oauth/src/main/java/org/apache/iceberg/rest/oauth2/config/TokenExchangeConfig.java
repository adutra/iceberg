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
package org.apache.iceberg.rest.oauth2.config;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOption;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOptions;
import org.apache.iceberg.rest.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.token.TypedToken;
import org.immutables.value.Value;

/**
 * Configuration for OAuth2 token exchange.
 *
 * <p>Note: this configuration is not used for token refreshes when the OAuth2 dialect is {@link
 * Dialect#ICEBERG_REST}.
 */
@Value.Immutable
@OAuth2ImmutableStyle
public interface TokenExchangeConfig {

  TokenExchangeConfig DEFAULT = builder().build();

  /**
   * The subject token to exchange.
   *
   * <p>If this value is present, the subject token will be used as-is. If this value is not
   * present, the subject token will be dynamically fetched using the configuration provided under
   * {@link #subjectTokenConfig()}.
   *
   * @see OAuth2Properties.TokenExchange#SUBJECT_TOKEN
   */
  Optional<String> subjectToken();

  /**
   * The type of the subject token. Must be a valid URN. The default is {@link
   * TypedToken#URN_ACCESS_TOKEN}.
   *
   * <p>If the agent is configured to dynamically fetch the subject token, this property is ignored
   * since only access tokens can be dynamically fetched.
   *
   * @see OAuth2Properties.TokenExchange#SUBJECT_TOKEN_TYPE
   */
  @Value.Default
  default URI subjectTokenType() {
    return TypedToken.URN_ACCESS_TOKEN;
  }

  /**
   * The actor token to exchange.
   *
   * <p>If this value is present, the actor token will be used as-is. If this value is not present,
   * the actor token will be dynamically fetched using the configuration provided under {@link
   * #actorTokenConfig()}. If no configuration is provided, no actor token will be used.
   *
   * @see OAuth2Properties.TokenExchange#ACTOR_TOKEN
   */
  Optional<String> actorToken();

  /**
   * The type of the actor token. Must be a valid URN. The default is {@link
   * TypedToken#URN_ACCESS_TOKEN}.
   *
   * <p>If the agent is configured to dynamically fetch the actor token, this property is ignored
   * since only access tokens can be dynamically fetched.
   *
   * @see OAuth2Properties.TokenExchange#ACTOR_TOKEN_TYPE
   */
  @Value.Default
  default URI actorTokenType() {
    return TypedToken.URN_ACCESS_TOKEN;
  }

  /**
   * The type of the requested security token. By default, {@link TypedToken#URN_ACCESS_TOKEN}.
   *
   * @see OAuth2Properties.TokenExchange#REQUESTED_TOKEN_TYPE
   */
  @Value.Default
  default URI requestedTokenType() {
    return TypedToken.URN_ACCESS_TOKEN;
  }

  /**
   * A URI that indicates the target service or resource where the client intends to use the
   * requested security token.
   *
   * @see OAuth2Properties.TokenExchange#RESOURCE
   */
  Optional<URI> resource();

  /**
   * The logical name of the target service where the client intends to use the requested security
   * token. This serves a purpose similar to the resource parameter but with the client providing a
   * logical name for the target service.
   *
   * @see OAuth2Properties.TokenExchange#AUDIENCE
   */
  Optional<String> audience();

  /**
   * The configuration to use for fetching the subject token. Required if {@link #subjectToken()} is
   * not set.
   *
   * <p>The properties returned by this method are merged with the main agent spec in order to
   * create the agent configuration. Therefore, the returned map does not need to contain a complete
   * agent configuration; it should only contain additional properties, or property overrides,
   * required to create the subject token fetch agent.
   *
   * <p>Note: validation of this configuration is done lazily, when the token is actually fetched.
   */
  Map<String, String> subjectTokenConfig();

  /**
   * The configuration to use for fetching the actor token. Required if {@link #actorToken()} is not
   * set but an actor token is required.
   *
   * <p>The properties returned by this method are merged with the main agent spec in order to
   * create the agent configuration. Therefore, the returned map does not need to contain a complete
   * agent configuration; it should only contain additional properties, or property overrides,
   * required to create the actor token fetch agent.
   *
   * <p>Note: validation of this configuration is done lazily, when the token is actually fetched.
   */
  Map<String, String> actorTokenConfig();

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (subjectToken().isEmpty()) {
      validator.check(
          subjectTokenType().equals(TypedToken.URN_ACCESS_TOKEN),
          OAuth2Properties.TokenExchange.SUBJECT_TOKEN_TYPE,
          "subject token type must be %s when using dynamic subject token",
          TypedToken.URN_ACCESS_TOKEN);
    }

    if (actorToken().isEmpty()) {
      validator.check(
          actorTokenType().equals(TypedToken.URN_ACCESS_TOKEN),
          OAuth2Properties.TokenExchange.ACTOR_TOKEN_TYPE,
          "actor token type must be %s when using dynamic actor token",
          TypedToken.URN_ACCESS_TOKEN);
    }

    validator.validate();
  }

  /** Merges the given properties into this {@link TokenExchangeConfig} and returns the result. */
  default TokenExchangeConfig merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Builder builder = builder();
    builder.resourceOption().set(properties, resource());
    builder.audienceOption().set(properties, audience());
    builder.subjectTokenOption().set(properties, subjectToken());
    builder.actorTokenOption().set(properties, actorToken());
    builder.subjectTokenTypeOption().set(properties, subjectTokenType());
    builder.actorTokenTypeOption().set(properties, actorTokenType());
    builder.subjectTokenConfigOption().set(properties, subjectTokenConfig());
    builder.actorTokenConfigOption().set(properties, actorTokenConfig());
    builder.requestedTokenTypeOption().set(properties, requestedTokenType());
    return builder.build();
  }

  static Builder builder() {
    return ImmutableTokenExchangeConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(TokenExchangeConfig config);

    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      resourceOption().set(properties);
      audienceOption().set(properties);
      subjectTokenOption().set(properties);
      actorTokenOption().set(properties);
      subjectTokenTypeOption().set(properties);
      actorTokenTypeOption().set(properties);
      subjectTokenConfigOption().set(properties);
      actorTokenConfigOption().set(properties);
      requestedTokenTypeOption().set(properties);
      return this;
    }

    @CanIgnoreReturnValue
    Builder requestedTokenType(URI tokenType);

    @CanIgnoreReturnValue
    Builder resource(URI resource);

    @CanIgnoreReturnValue
    Builder audience(String audience);

    @CanIgnoreReturnValue
    Builder subjectToken(String token);

    @CanIgnoreReturnValue
    Builder actorToken(String token);

    @CanIgnoreReturnValue
    Builder subjectTokenType(URI tokenType);

    @CanIgnoreReturnValue
    Builder actorTokenType(URI tokenType);

    @CanIgnoreReturnValue
    Builder subjectTokenConfig(Map<String, ? extends String> config);

    @CanIgnoreReturnValue
    Builder actorTokenConfig(Map<String, ? extends String> config);

    TokenExchangeConfig build();

    private ConfigOption<URI> resourceOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenExchange.RESOURCE, this::resource, URI::create);
    }

    private ConfigOption<String> audienceOption() {
      return ConfigOptions.simple(OAuth2Properties.TokenExchange.AUDIENCE, this::audience);
    }

    private ConfigOption<String> subjectTokenOption() {
      return ConfigOptions.simple(OAuth2Properties.TokenExchange.SUBJECT_TOKEN, this::subjectToken);
    }

    private ConfigOption<String> actorTokenOption() {
      return ConfigOptions.simple(OAuth2Properties.TokenExchange.ACTOR_TOKEN, this::actorToken);
    }

    private ConfigOption<URI> subjectTokenTypeOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenExchange.SUBJECT_TOKEN_TYPE, this::subjectTokenType, URI::create);
    }

    private ConfigOption<URI> actorTokenTypeOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenExchange.ACTOR_TOKEN_TYPE, this::actorTokenType, URI::create);
    }

    private ConfigOption<Map<String, String>> subjectTokenConfigOption() {
      return ConfigOptions.prefixMap(
          OAuth2Properties.TokenExchange.SUBJECT_CONFIG_PREFIX,
          OAuth2Properties.PREFIX,
          this::subjectTokenConfig);
    }

    private ConfigOption<Map<String, String>> actorTokenConfigOption() {
      return ConfigOptions.prefixMap(
          OAuth2Properties.TokenExchange.ACTOR_CONFIG_PREFIX,
          OAuth2Properties.PREFIX,
          this::actorTokenConfig);
    }

    private ConfigOption<URI> requestedTokenTypeOption() {
      return ConfigOptions.simple(
          OAuth2Properties.TokenExchange.REQUESTED_TOKEN_TYPE,
          this::requestedTokenType,
          URI::create);
    }
  }
}
