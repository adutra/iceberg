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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtils;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigValidator;
import org.apache.iceberg.rest.auth.oauth2.config.DeviceCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableAuthorizationCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableDeviceCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableHttpClientConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableResourceOwnerConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableTokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableTokenRefreshConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ResourceOwnerConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig;
import org.immutables.value.Value;

/** The configuration for the OAuth2 AuthManager. */
@Value.Immutable(prehash = true) // prehash for use as cache key
public interface OAuth2Config {

  String PREFIX = "rest.auth.oauth2.";

  /**
   * The basic configuration, including token endpoint, grant type, client id and client secret.
   * Required.
   */
  BasicConfig basicConfig();

  /** The token refresh configuration. Optional. */
  @Value.Default
  default TokenRefreshConfig tokenRefreshConfig() {
    return ImmutableTokenRefreshConfig.builder().build();
  }

  /** The resource owner configuration. Required for the {@link GrantType#PASSWORD} grant type. */
  @Value.Default
  default ResourceOwnerConfig resourceOwnerConfig() {
    return ImmutableResourceOwnerConfig.builder().build();
  }

  /**
   * The authorization code configuration. Required for the {@link GrantType#AUTHORIZATION_CODE}
   * grant type.
   */
  @Value.Default
  default AuthorizationCodeConfig authorizationCodeConfig() {
    return ImmutableAuthorizationCodeConfig.builder().build();
  }

  /** The device code configuration. Required for the {@link GrantType#DEVICE_CODE} grant type. */
  @Value.Default
  default DeviceCodeConfig deviceCodeConfig() {
    return ImmutableDeviceCodeConfig.builder().build();
  }

  /** The token exchange configuration. Optional. */
  @Value.Default
  default TokenExchangeConfig tokenExchangeConfig() {
    return ImmutableTokenExchangeConfig.builder().build();
  }

  /**
   * The client JWT assertion configuration. Required when the client authentication method is
   * {@link ClientAuthenticationMethod#CLIENT_SECRET_JWT} or {@link
   * ClientAuthenticationMethod#PRIVATE_KEY_JWT}.
   */
  @Value.Default
  default ClientAssertionConfig clientAssertionConfig() {
    return ImmutableClientAssertionConfig.builder().build();
  }

  @Value.Default
  default HttpClientConfig httpClientConfig() {
    return ImmutableHttpClientConfig.builder().build();
  }

  /** Creates an {@link OAuth2Config} builder from the given properties map. */
  static OAuth2Config fromProperties(Map<String, String> properties) {
    return ImmutableOAuth2Config.builder()
        .basicConfig(BasicConfig.fromProperties(properties).build())
        .tokenRefreshConfig(TokenRefreshConfig.fromProperties(properties).build())
        .resourceOwnerConfig(ResourceOwnerConfig.fromProperties(properties).build())
        .authorizationCodeConfig(AuthorizationCodeConfig.fromProperties(properties).build())
        .deviceCodeConfig(DeviceCodeConfig.fromProperties(properties).build())
        .tokenExchangeConfig(TokenExchangeConfig.fromProperties(properties).build())
        .clientAssertionConfig(ClientAssertionConfig.fromProperties(properties).build())
        .httpClientConfig(HttpClientConfig.fromProperties(properties).build())
        .build();
  }

  @Value.Check
  default void validate() {
    // We only need to validate constraints that span multiple configuration options here;
    // individual configuration options are validated in their respective classes.
    ConfigValidator validator = new ConfigValidator();
    GrantType grantType = basicConfig().grantType();
    if (grantType.equals(GrantType.PASSWORD)) {
      validator.check(
          resourceOwnerConfig().username().isPresent()
              && !resourceOwnerConfig().username().get().isEmpty(),
          ResourceOwnerConfig.PREFIX + ResourceOwnerConfig.USERNAME,
          "username must be set if grant type is '%s'",
          GrantType.PASSWORD.getValue());
      validator.check(
          resourceOwnerConfig().password().isPresent(),
          ResourceOwnerConfig.PREFIX + ResourceOwnerConfig.PASSWORD,
          "password must be set if grant type is '%s'",
          GrantType.PASSWORD.getValue());
    }

    if (grantType.equals(GrantType.AUTHORIZATION_CODE)) {
      validator.check(
          basicConfig().issuerUrl().isPresent()
              || authorizationCodeConfig().authorizationEndpoint().isPresent(),
          List.of(
              PREFIX + BasicConfig.ISSUER_URL,
              AuthorizationCodeConfig.PREFIX + AuthorizationCodeConfig.ENDPOINT),
          "either issuer URL or authorization endpoint must be set if grant type is '%s'",
          GrantType.AUTHORIZATION_CODE.getValue());
    }

    if (grantType.equals(GrantType.DEVICE_CODE)) {
      validator.check(
          basicConfig().issuerUrl().isPresent()
              || deviceCodeConfig().deviceAuthorizationEndpoint().isPresent(),
          List.of(
              PREFIX + BasicConfig.ISSUER_URL, DeviceCodeConfig.PREFIX + DeviceCodeConfig.ENDPOINT),
          "either issuer URL or device authorization endpoint must be set if grant type is '%s'",
          GrantType.DEVICE_CODE.getValue());
    }

    ClientAuthenticationMethod method = basicConfig().clientAuthenticationMethod();
    if (ConfigUtils.requiresJwsAlgorithm(method)) {
      if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_JWT)) {
        if (clientAssertionConfig().algorithm().isPresent()) {
          validator.check(
              JWSAlgorithm.Family.HMAC_SHA.contains(clientAssertionConfig().algorithm().get()),
              List.of(
                  PREFIX + BasicConfig.CLIENT_AUTH,
                  ClientAssertionConfig.PREFIX + ClientAssertionConfig.ALGORITHM),
              "client authentication method '%s' is not compatible with JWS algorithm '%s'",
              method.getValue(),
              clientAssertionConfig().algorithm().get());
        }

        validator.check(
            clientAssertionConfig().privateKey().isEmpty(),
            List.of(
                PREFIX + BasicConfig.CLIENT_AUTH,
                ClientAssertionConfig.PREFIX + ClientAssertionConfig.PRIVATE_KEY),
            "client authentication method '%s' must not have a private key configured",
            method.getValue());
      }

      if (method.equals(ClientAuthenticationMethod.PRIVATE_KEY_JWT)) {
        if (clientAssertionConfig().algorithm().isPresent()) {
          validator.check(
              JWSAlgorithm.Family.SIGNATURE.contains(clientAssertionConfig().algorithm().get()),
              List.of(
                  PREFIX + BasicConfig.CLIENT_AUTH,
                  ClientAssertionConfig.PREFIX + ClientAssertionConfig.ALGORITHM),
              "client authentication method '%s' is not compatible with JWS algorithm '%s'",
              method.getValue(),
              clientAssertionConfig().algorithm().get());
        }

        validator.check(
            clientAssertionConfig().privateKey().isPresent(),
            List.of(
                PREFIX + BasicConfig.CLIENT_AUTH,
                ClientAssertionConfig.PREFIX + ClientAssertionConfig.PRIVATE_KEY),
            "client authentication method '%s' requires a private key",
            method.getValue());
      }
    }

    validator.validate();
  }
}
