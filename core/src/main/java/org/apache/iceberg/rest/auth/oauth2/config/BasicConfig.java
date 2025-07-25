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
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Basic OAuth2 properties. These properties are used to configure the basic OAuth2 options such as
 * the issuer URL, token endpoint, client ID, and client secret.
 */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface BasicConfig {

  String PREFIX = OAuth2Config.PREFIX;

  String TOKEN = "token";
  String ISSUER_URL = "issuer-url";
  String TOKEN_ENDPOINT = "token-endpoint";
  String GRANT_TYPE = "grant-type";
  String CLIENT_ID = "client-id";
  String CLIENT_AUTH = "client-auth";
  String CLIENT_SECRET = "client-secret";
  String SCOPE = "scope";
  String EXTRA_PARAMS = "extra-params";
  String TIMEOUT = "timeout";
  String SESSION_CACHE_TIMEOUT = "session-cache-timeout";

  GrantType DEFAULT_GRANT_TYPE = GrantType.CLIENT_CREDENTIALS;
  ClientAuthenticationMethod DEFAULT_CLIENT_AUTH = ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
  Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);
  Duration DEFAULT_SESSION_CACHE_TIMEOUT = Duration.ofHours(1);

  /**
   * The initial access token to use. Optional. If this is set, the OAuth2 client will not attempt
   * to fetch an initial new token from the Authorization server, but will use this token instead.
   *
   * <p>This option should be avoided as in most cases, the token cannot be refreshed.
   */
  @ConfigOption(TOKEN)
  @Value.Redacted
  Optional<TypelessAccessToken> token();

  /**
   * The root URL of the Authorization server, which will be used for discovering supported
   * endpoints and their locations. For Keycloak, this is typically the realm URL: {@code
   * https://<keycloak-server>/realms/<realm-name>}.
   *
   * <p>Two "well-known" paths are supported for endpoint discovery: {@code
   * .well-known/openid-configuration} and {@code .well-known/oauth-authorization-server}. The full
   * metadata discovery URL will be constructed by appending these paths to the issuer URL.
   *
   * <p>Either this property or {@link #TOKEN_ENDPOINT} must be set.
   *
   * @see <a
   *     href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata">OpenID
   *     Connect Discovery 1.0</a>
   * @see <a href="https://tools.ietf.org/html/rfc8414#section-5">RFC 8414 Section 5</a>
   */
  @ConfigOption(ISSUER_URL)
  Optional<URI> issuerUrl();

  /**
   * URL of the OAuth2 token endpoint. For Keycloak, this is typically {@code
   * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token}.
   *
   * <p>Either this property or {@link #ISSUER_URL} must be set. In case it is not set, the token
   * endpoint will be discovered from the {@link #ISSUER_URL issuer URL}, using the OpenID Connect
   * Discovery metadata published by the issuer.
   */
  @ConfigOption(TOKEN_ENDPOINT)
  Optional<URI> tokenEndpoint();

  /**
   * The grant type to use when authenticating against the OAuth2 server. Valid values are:
   *
   * <ul>
   *   <li>{@link GrantType#CLIENT_CREDENTIALS client_credentials}
   *   <li>{@link GrantType#PASSWORD password}
   *   <li>{@link GrantType#AUTHORIZATION_CODE authorization_code}
   *   <li>{@link GrantType#DEVICE_CODE urn:ietf:params:oauth:grant-type:device_code}
   *   <li>{@link GrantType#TOKEN_EXCHANGE urn:ietf:params:oauth:grant-type:token-exchange}
   * </ul>
   *
   * Optional, defaults to {@link #DEFAULT_GRANT_TYPE}.
   */
  @ConfigOption(GRANT_TYPE)
  @Value.Default
  default GrantType grantType() {
    return DEFAULT_GRANT_TYPE;
  }

  /**
   * Client ID to use when authenticating against the OAuth2 server. Required, unless a {@linkplain
   * #TOKEN static token} is provided.
   */
  @ConfigOption(CLIENT_ID)
  Optional<ClientID> clientId();

  /**
   * The OAuth2 client authentication method to use. Valid values are:
   *
   * <ul>
   *   <li>{@link ClientAuthenticationMethod#NONE none}: the client does not authenticate itself at
   *       the token endpoint, because it is a public client with no client secret or other
   *       authentication mechanism.
   *   <li>{@link ClientAuthenticationMethod#CLIENT_SECRET_BASIC client_secret_basic}: client secret
   *       is sent in the HTTP Basic Authorization header.
   *   <li>{@link ClientAuthenticationMethod#CLIENT_SECRET_POST client_secret_post}: client secret
   *       is sent in the request body as a form parameter.
   *   <li>{@link ClientAuthenticationMethod#CLIENT_SECRET_JWT client_secret_jwt}: client secret is
   *       used to sign a JWT token.
   *   <li>{@link ClientAuthenticationMethod#PRIVATE_KEY_JWT private_key_jwt}: client authenticates
   *       with a JWT assertion signed with a private key.
   * </ul>
   *
   * The default is {@link #DEFAULT_CLIENT_AUTH}.
   */
  @ConfigOption(CLIENT_AUTH)
  @Value.Default
  default ClientAuthenticationMethod clientAuthenticationMethod() {
    return DEFAULT_CLIENT_AUTH;
  }

  /**
   * Client secret to use when authenticating against the OAuth2 server. Required if the client is
   * private and is authenticated using the standard "client-secret" methods. If other
   * authentication methods are used (e.g. {@code private_key_jwt}), this property is ignored.
   */
  @ConfigOption(CLIENT_SECRET)
  @Value.Redacted
  Optional<Secret> clientSecret();

  /**
   * Space-separated list of scopes to include in each request to the OAuth2 server. Optional,
   * defaults to empty (no scopes).
   *
   * <p>The scope names will not be validated by the OAuth2 client; make sure they are valid
   * according to <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">RFC 6749
   * Section 3.3</a>.
   */
  @ConfigOption(SCOPE)
  Optional<Scope> scope();

  /**
   * Extra parameters to include in each request to the token and device authorization endpoints.
   * This is useful for custom parameters that are not covered by the standard OAuth2.0
   * specification. Optional, defaults to empty.
   *
   * <p>This is a prefix property, and multiple values can be set, each with a different key and
   * value. The values must NOT be URL-encoded. Example:
   *
   * <pre>{@code
   * rest.auth.oauth2.extra-params.custom_param1=custom_value1"
   * rest.auth.oauth2.extra-params.custom_param2=custom_value2"
   * }</pre>
   *
   * For example, Auth0 requires the {@code audience} parameter to be set to the API identifier.
   * This can be done by setting the following configuration:
   *
   * <pre>{@code
   * rest.auth.oauth2.extra-params.audience=https://iceberg-rest-catalog/api
   * }</pre>
   */
  @ConfigOption(value = EXTRA_PARAMS, prefixMap = true)
  Map<String, String> extraRequestParameters();

  /**
   * Defines how long the OAuth2 client should wait for tokens to be acquired. Optional, defaults to
   * {@link #DEFAULT_TIMEOUT}.
   *
   * <p>Must be a valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601
   * duration</a>.
   */
  @ConfigOption(TIMEOUT)
  @Value.Default
  default Duration timeout() {
    return DEFAULT_TIMEOUT;
  }

  /**
   * The session cache timeout. Cached sessions will become eligible for eviction after this
   * duration of inactivity. Defaults to 1 hour. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   *
   * <p>This value is used for housekeeping; it does not mean that cached sessions will stop working
   * after this time, but that the session cache will evict the session after this time of
   * inactivity. If the context is used again, a new session will be created and cached.
   */
  @ConfigOption(SESSION_CACHE_TIMEOUT)
  @Value.Default
  default Duration sessionCacheTimeout() {
    return DEFAULT_SESSION_CACHE_TIMEOUT;
  }

  /**
   * The minimum timeout for token acquisition.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  @Value.Default
  default Duration minTimeout() {
    return Duration.ofSeconds(30);
  }

  /**
   * The distinctive name of the OAuth2 client. Defaults to {@code iceberg-oauth2-client}. This name
   * is printed in all log messages and user prompts.
   *
   * <p>This option is not exposed as a public configuration property, and is intended for testing
   * purposes only.
   */
  Optional<String> clientName();

  @Value.Check
  default BasicConfig validate() {
    ConfigValidator validator = new ConfigValidator();
    BasicConfig basicConfig = this;
    validator.check(
        issuerUrl().isPresent() || tokenEndpoint().isPresent(),
        List.of(PREFIX + ISSUER_URL, PREFIX + TOKEN_ENDPOINT),
        "either issuer URL or token endpoint must be set");
    if (issuerUrl().isPresent()) {
      validator.checkEndpoint(issuerUrl().get(), PREFIX + ISSUER_URL, "Issuer URL");
    }

    if (tokenEndpoint().isPresent()) {
      validator.checkEndpoint(tokenEndpoint().get(), PREFIX + TOKEN_ENDPOINT, "Token endpoint");
    }

    validator.check(
        ConfigUtils.SUPPORTED_INITIAL_GRANT_TYPES.contains(grantType()),
        PREFIX + GRANT_TYPE,
        "grant type must be one of: %s",
        ConfigUtils.SUPPORTED_INITIAL_GRANT_TYPES.stream()
            .map(GrantType::getValue)
            .collect(Collectors.joining("', '", "'", "'")));
    validator.check(
        ConfigUtils.SUPPORTED_CLIENT_AUTH_METHODS.contains(clientAuthenticationMethod()),
        PREFIX + CLIENT_AUTH,
        "client authentication method must be one of: %s",
        ConfigUtils.SUPPORTED_CLIENT_AUTH_METHODS.stream()
            .map(ClientAuthenticationMethod::getValue)
            .collect(Collectors.joining("', '", "'", "'")));
    // Only validate client ID and client secret if a token is not provided
    if (token().isEmpty()) {
      validator.check(clientId().isPresent(), PREFIX + CLIENT_ID, "client ID must not be empty");
      if (ConfigUtils.requiresClientSecret(clientAuthenticationMethod())) {
        validator.check(
            clientSecret().isPresent(),
            List.of(PREFIX + CLIENT_AUTH, PREFIX + CLIENT_SECRET),
            "client secret must not be empty when client authentication is '%s'",
            clientAuthenticationMethod().getValue());
      } else if (clientAuthenticationMethod().equals(ClientAuthenticationMethod.PRIVATE_KEY_JWT)) {
        validator.check(
            clientSecret().isEmpty(),
            List.of(PREFIX + CLIENT_AUTH, PREFIX + CLIENT_SECRET),
            "client secret must not be set when client authentication is '%s'",
            clientAuthenticationMethod().getValue());
      } else if (clientAuthenticationMethod().equals(ClientAuthenticationMethod.NONE)) {
        validator.check(
            clientSecret().isEmpty(),
            List.of(PREFIX + CLIENT_AUTH, PREFIX + CLIENT_SECRET),
            "client secret must not be set when client authentication is '%s'",
            ClientAuthenticationMethod.NONE.getValue());
        validator.check(
            !grantType().equals(DEFAULT_GRANT_TYPE),
            List.of(PREFIX + CLIENT_AUTH, PREFIX + GRANT_TYPE),
            "grant type must not be '%s' when client authentication is '%s'",
            DEFAULT_GRANT_TYPE.getValue(),
            ClientAuthenticationMethod.NONE.getValue());
      }
    }

    validator.check(
        timeout().compareTo(minTimeout()) >= 0,
        PREFIX + TIMEOUT,
        "timeout must be greater than or equal to %s",
        minTimeout());

    validator.validate();
    return basicConfig;
  }

  static ImmutableBasicConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    List<String> scopes = ConfigUtils.parseList(props, SCOPE, " ");
    return ImmutableBasicConfig.builder()
        .token(ConfigUtils.parseOptional(props, TOKEN, TypelessAccessToken::new))
        .issuerUrl(ConfigUtils.parseOptional(props, ISSUER_URL, URI::create))
        .tokenEndpoint(ConfigUtils.parseOptional(props, TOKEN_ENDPOINT, URI::create))
        .grantType(
            ConfigUtils.parseOptional(props, GRANT_TYPE, GrantType::parse)
                .orElse(DEFAULT_GRANT_TYPE))
        .clientAuthenticationMethod(
            ConfigUtils.parseOptional(props, CLIENT_AUTH, ClientAuthenticationMethod::parse)
                .orElse(DEFAULT_CLIENT_AUTH))
        .clientId(ConfigUtils.parseOptional(props, CLIENT_ID, ClientID::new))
        .clientSecret(ConfigUtils.parseOptional(props, CLIENT_SECRET, Secret::new))
        .scope(
            scopes.isEmpty()
                ? Optional.empty()
                : Optional.of(new Scope(scopes.toArray(String[]::new))))
        .extraRequestParameters(RESTUtil.extractPrefixMap(props, EXTRA_PARAMS + '.'))
        .timeout(ConfigUtils.parseOptional(props, TIMEOUT, Duration::parse).orElse(DEFAULT_TIMEOUT))
        .sessionCacheTimeout(
            ConfigUtils.parseOptional(props, SESSION_CACHE_TIMEOUT, Duration::parse)
                .orElse(DEFAULT_SESSION_CACHE_TIMEOUT));
  }
}
