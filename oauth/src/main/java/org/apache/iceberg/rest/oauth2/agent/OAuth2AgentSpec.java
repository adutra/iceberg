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
package org.apache.iceberg.rest.oauth2.agent;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.oauth2.OAuth2Properties.AuthorizationCode;
import org.apache.iceberg.rest.oauth2.OAuth2Properties.DeviceCode;
import org.apache.iceberg.rest.oauth2.config.AuthorizationCodeConfig;
import org.apache.iceberg.rest.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.oauth2.config.DeviceCodeConfig;
import org.apache.iceberg.rest.oauth2.config.ResourceOwnerPasswordConfig;
import org.apache.iceberg.rest.oauth2.config.RuntimeConfig;
import org.apache.iceberg.rest.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.oauth2.config.TokenRefreshConfig;
import org.apache.iceberg.rest.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public interface OAuth2AgentSpec {

  /**
   * The basic configuration, including token endpoint, grant type, client id and client secret.
   * Required.
   */
  BasicConfig basicConfig();

  /** The resource owner configuration. Required for the {@link GrantType#PASSWORD} grant type. */
  @Value.Default
  default ResourceOwnerPasswordConfig resourceOwnerPasswordConfig() {
    return ResourceOwnerPasswordConfig.DEFAULT;
  }

  /**
   * The authorization code configuration. Required for the {@link GrantType#AUTHORIZATION_CODE}
   * grant type.
   */
  @Value.Default
  default AuthorizationCodeConfig authorizationCodeConfig() {
    return AuthorizationCodeConfig.DEFAULT;
  }

  /** The device code configuration. Required for the {@link GrantType#DEVICE_CODE} grant type. */
  @Value.Default
  default DeviceCodeConfig deviceCodeConfig() {
    return DeviceCodeConfig.DEFAULT;
  }

  /** The token refresh configuration. Optional. */
  @Value.Default
  default TokenRefreshConfig tokenRefreshConfig() {
    return TokenRefreshConfig.DEFAULT;
  }

  /** The token exchange configuration. Optional. */
  @Value.Default
  default TokenExchangeConfig tokenExchangeConfig() {
    return TokenExchangeConfig.DEFAULT;
  }

  /** The runtime configuration. Optional. */
  @Value.Default
  default RuntimeConfig runtimeConfig() {
    return RuntimeConfig.DEFAULT;
  }

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    // We only need to validate constraints that span multiple configuration options here;
    // individual configuration options are validated in their respective classes.
    if (basicConfig().grantType() == GrantType.PASSWORD) {
      validator.check(
          resourceOwnerPasswordConfig().username().isPresent()
              && !resourceOwnerPasswordConfig().username().get().isEmpty(),
          OAuth2Properties.ResourceOwnerPassword.USERNAME,
          "username must be set if grant type is '%s'",
          GrantType.PASSWORD.commonName());
      validator.check(
          resourceOwnerPasswordConfig().password().isPresent(),
          OAuth2Properties.ResourceOwnerPassword.PASSWORD,
          "password must be set if grant type is '%s'",
          GrantType.PASSWORD.commonName());
    }

    if (basicConfig().grantType() == GrantType.AUTHORIZATION_CODE) {
      validator.check(
          basicConfig().issuerUrl().isPresent()
              || authorizationCodeConfig().authorizationEndpoint().isPresent(),
          List.of(OAuth2Properties.Basic.ISSUER_URL, AuthorizationCode.ENDPOINT),
          "either issuer URL or authorization endpoint must be set if grant type is '%s'",
          GrantType.AUTHORIZATION_CODE.commonName());
    }

    if (basicConfig().grantType() == GrantType.DEVICE_CODE) {
      validator.check(
          basicConfig().issuerUrl().isPresent()
              || deviceCodeConfig().deviceAuthorizationEndpoint().isPresent(),
          List.of(OAuth2Properties.Basic.ISSUER_URL, DeviceCode.ENDPOINT),
          "either issuer URL or device authorization endpoint must be set if grant type is '%s'",
          GrantType.DEVICE_CODE.commonName());
    }

    validator.validate();
  }

  /** Merges the given properties into this {@link OAuth2AgentSpec} and returns the result. */
  default OAuth2AgentSpec merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    return builder()
        .basicConfig(basicConfig().merge(properties))
        .resourceOwnerPasswordConfig(resourceOwnerPasswordConfig().merge(properties))
        .authorizationCodeConfig(authorizationCodeConfig().merge(properties))
        .deviceCodeConfig(deviceCodeConfig().merge(properties))
        .tokenRefreshConfig(tokenRefreshConfig().merge(properties))
        .tokenExchangeConfig(tokenExchangeConfig().merge(properties))
        .runtimeConfig(runtimeConfig().merge(properties))
        .build();
  }

  static Builder builder() {
    return ImmutableOAuth2AgentSpec.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(OAuth2AgentSpec spec);

    /**
     * Configures this {@link Builder} with the given properties.
     *
     * @throws NullPointerException if {@code properties} is {@code null}, or a required
     *     configuration option is missing
     * @throws IllegalArgumentException if the configuration is otherwise invalid
     * @see OAuth2Properties
     */
    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      return basicConfig(BasicConfig.builder().from(properties).build())
          .resourceOwnerPasswordConfig(
              ResourceOwnerPasswordConfig.builder().from(properties).build())
          .authorizationCodeConfig(AuthorizationCodeConfig.builder().from(properties).build())
          .deviceCodeConfig(DeviceCodeConfig.builder().from(properties).build())
          .tokenRefreshConfig(TokenRefreshConfig.builder().from(properties).build())
          .tokenExchangeConfig(TokenExchangeConfig.builder().from(properties).build())
          .runtimeConfig(RuntimeConfig.builder().from(properties).build());
    }

    @CanIgnoreReturnValue
    Builder basicConfig(BasicConfig basicConfig);

    @CanIgnoreReturnValue
    Builder resourceOwnerPasswordConfig(ResourceOwnerPasswordConfig resourceOwnerPasswordConfig);

    @CanIgnoreReturnValue
    Builder authorizationCodeConfig(AuthorizationCodeConfig authorizationCodeConfig);

    @CanIgnoreReturnValue
    Builder deviceCodeConfig(DeviceCodeConfig deviceCodeConfig);

    @CanIgnoreReturnValue
    Builder tokenRefreshConfig(TokenRefreshConfig tokenRefreshConfig);

    @CanIgnoreReturnValue
    Builder tokenExchangeConfig(TokenExchangeConfig tokenExchangeConfig);

    @CanIgnoreReturnValue
    Builder runtimeConfig(RuntimeConfig runtimeConfig);

    OAuth2AgentSpec build();
  }
}
