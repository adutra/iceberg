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
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Configuration properties for the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.1">Authorization Code Grant</a>
 * flow.
 *
 * <p>This flow is used to obtain an access token by redirecting the user to the OAuth2
 * authorization server, where they can log in and authorize the client application to access their
 * resources.
 */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface AuthorizationCodeConfig {

  String GROUP_NAME = "auth-code";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String ENDPOINT = "endpoint";
  String REDIRECT_URI = "redirect-uri";

  String CALLBACK_HTTPS = "callback.https";
  String CALLBACK_BIND_HOST = "callback.bind-host";
  String CALLBACK_BIND_PORT = "callback.bind-port";
  String CALLBACK_CONTEXT_PATH = "callback.context-path";

  String PKCE_ENABLED = "pkce.enabled";
  String PKCE_METHOD = "pkce.method";

  String SSL_KEYSTORE_PATH = "ssl.key-store.path";
  String SSL_KEYSTORE_PASSWORD = "ssl.key-store.password";
  String SSL_KEYSTORE_ALIAS = "ssl.key-store.alias";
  String SSL_PROTOCOLS = "ssl.protocols";
  String SSL_CIPHER_SUITES = "ssl.cipher-suites";

  CodeChallengeMethod DEFAULT_CODE_CHALLENGE_METHOD = CodeChallengeMethod.S256;

  /**
   * URL of the OAuth2 authorization endpoint. For Keycloak, this is typically {@code
   * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/auth}.
   *
   * <p>If using the "authorization_code" grant type, either this property or {@link
   * BasicConfig#ISSUER_URL} must be set. In case it is not set, the authorization endpoint will be
   * discovered from the {@link BasicConfig#ISSUER_URL issuer URL}, using the OpenID Connect
   * Discovery metadata published by the issuer.
   */
  @ConfigOption(ENDPOINT)
  Optional<URI> authorizationEndpoint();

  /**
   * The redirect URI. This is the value of the {@code redirect_uri} parameter in the authorization
   * code request.
   *
   * <p>Optional; if not present, the URL will be computed from {@value #CALLBACK_BIND_HOST},
   * {@value #CALLBACK_BIND_PORT} and {@value #CALLBACK_CONTEXT_PATH}.
   *
   * <p>Specifying this value is generally only necessary in containerized environments, if a
   * reverse proxy modifies the callback before it reaches the client, or if external TLS
   * termination is performed.
   */
  @ConfigOption(REDIRECT_URI)
  Optional<URI> redirectUri();

  /**
   * Whether to use HTTPS for the local web server that listens for the authorization code. The
   * default is {@code false}.
   *
   * <p>Ignored if {@value #REDIRECT_URI} is set.
   */
  @ConfigOption(CALLBACK_HTTPS)
  @Value.Default
  default boolean callbackHttps() {
    return false;
  }

  /**
   * Address of the OAuth2 authorization code flow local web server.
   *
   * <p>Ignored if {@value #REDIRECT_URI} is set.
   *
   * <p>The internal web server will listen for the authorization code callback on this address.
   * This is only used if the grant type to use is {@link GrantType#AUTHORIZATION_CODE}.
   *
   * <p>Optional; if not present, the server will listen on the loopback interface.
   */
  @ConfigOption(CALLBACK_BIND_HOST)
  Optional<String> callbackBindHost();

  /**
   * Port of the OAuth2 authorization code flow local web server.
   *
   * <p>Ignored if {@value #REDIRECT_URI} is set.
   *
   * <p>The internal web server will listen for the authorization code callback on this port. This
   * is only used if the grant type to use is {@link GrantType#AUTHORIZATION_CODE}.
   *
   * <p>Optional; if not present, a random port will be used.
   */
  @ConfigOption(CALLBACK_BIND_PORT)
  OptionalInt callbackBindPort();

  /**
   * Context path of the OAuth2 authorization code flow local web server.
   *
   * <p>Ignored if {@value #REDIRECT_URI} is set.
   *
   * <p>Optional; if not present, a default context path will be used.
   */
  @ConfigOption(CALLBACK_CONTEXT_PATH)
  Optional<String> callbackContextPath();

  /**
   * Whether to enable PKCE (Proof Key for Code Exchange) for the authorization code flow. The
   * default is {@code true}.
   *
   * @see <a href="https://www.rfc-editor.org/rfc/rfc7636">RFC 7636</a>
   */
  @ConfigOption(PKCE_ENABLED)
  @Value.Default
  default boolean pkceEnabled() {
    return true;
  }

  /**
   * The PKCE code challenge method to use. The default is {@link #DEFAULT_CODE_CHALLENGE_METHOD}.
   * This is only used if PKCE is enabled.
   *
   * @see <a href="https://www.rfc-editor.org/rfc/rfc7636#section-4.2">RFC 7636 Section 4.2</a>
   */
  @ConfigOption(PKCE_METHOD)
  @Value.Default
  default CodeChallengeMethod codeChallengeMethod() {
    return DEFAULT_CODE_CHALLENGE_METHOD;
  }

  /**
   * Path to the key store to use for HTTPS requests. Optional, defaults to the system key store.
   *
   * <p>Ignored if {@value #CALLBACK_HTTPS} is {@code false} or if {@value #REDIRECT_URI} is set to
   * a non-HTTPS URL.
   */
  @ConfigOption(SSL_KEYSTORE_PATH)
  Optional<Path> sslKeyStorePath();

  /**
   * Password for the key store to use for HTTPS requests. Optional, defaults to no password.
   *
   * <p>Ignored if {@value #CALLBACK_HTTPS} is {@code false} or if {@value #REDIRECT_URI} is set to
   * a non-HTTPS URL.
   */
  @ConfigOption(SSL_KEYSTORE_PASSWORD)
  @Value.Redacted
  Optional<String> sslKeyStorePassword();

  /**
   * The alias of the key to use from the key store. Optional, defaults to the first matching key in
   * the store.
   *
   * <p>Ignored if {@value #CALLBACK_HTTPS} is {@code false} or if {@value #REDIRECT_URI} is set to
   * a non-HTTPS URL.
   */
  @ConfigOption(SSL_KEYSTORE_ALIAS)
  Optional<String> sslKeyStoreAlias();

  /**
   * A comma-separated list of SSL protocols to use for HTTPS requests. Optional, defaults to the
   * system protocols.
   *
   * <p>Ignored if {@value #CALLBACK_HTTPS} is {@code false} or if {@value #REDIRECT_URI} is set to
   * a non-HTTPS URL.
   */
  @ConfigOption(SSL_PROTOCOLS)
  List<String> sslProtocols();

  /**
   * A comma-separated list of SSL cipher suites to use for HTTPS requests. Optional, defaults to
   * the system cipher suites.
   *
   * <p>Ignored if {@value #CALLBACK_HTTPS} is {@code false} or if {@value #REDIRECT_URI} is set to
   * a non-HTTPS URL.
   */
  @ConfigOption(SSL_CIPHER_SUITES)
  List<String> sslCipherSuites();

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (authorizationEndpoint().isPresent()) {
      validator.checkEndpoint(
          authorizationEndpoint().get(),
          PREFIX + ENDPOINT,
          "authorization code flow: authorization endpoint");
    }

    if (callbackBindPort().isPresent()) {
      validator.check(
          callbackBindPort().getAsInt() >= 0 && callbackBindPort().getAsInt() <= 65535,
          PREFIX + CALLBACK_BIND_PORT,
          "authorization code flow: callback bind port must be between 0 and 65535 (inclusive)");
    }

    if (pkceEnabled()) {
      validator.check(
          ConfigUtils.SUPPORTED_CODE_CHALLENGE_METHODS.contains(codeChallengeMethod()),
          PREFIX + PKCE_METHOD,
          "authorization code flow: code challenge method must be one of: %s",
          ConfigUtils.SUPPORTED_CODE_CHALLENGE_METHODS.stream()
              .map(CodeChallengeMethod::getValue)
              .collect(Collectors.joining("', '", "'", "'")));
    }

    if (sslKeyStorePath().isPresent()) {
      validator.check(
          Files.isReadable(sslKeyStorePath().get()),
          PREFIX + SSL_KEYSTORE_PATH,
          "authorization code flow: SSL keystore path '%s' is not a file or is not readable",
          sslKeyStorePath().get());
    }

    validator.validate();
  }

  static ImmutableAuthorizationCodeConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    return ImmutableAuthorizationCodeConfig.builder()
        .authorizationEndpoint(ConfigUtils.parseOptional(props, ENDPOINT, URI::create))
        .redirectUri(ConfigUtils.parseOptional(props, REDIRECT_URI, URI::create))
        .callbackHttps(
            ConfigUtils.parseOptional(props, CALLBACK_HTTPS, Boolean::parseBoolean).orElse(false))
        .callbackBindHost(ConfigUtils.parseOptional(props, CALLBACK_BIND_HOST))
        .callbackBindPort(ConfigUtils.parseOptionalInt(props, CALLBACK_BIND_PORT))
        .callbackContextPath(ConfigUtils.parseOptional(props, CALLBACK_CONTEXT_PATH))
        .pkceEnabled(
            ConfigUtils.parseOptional(props, PKCE_ENABLED, Boolean::parseBoolean).orElse(true))
        .codeChallengeMethod(
            ConfigUtils.parseOptional(props, PKCE_METHOD, CodeChallengeMethod::parse)
                .orElse(DEFAULT_CODE_CHALLENGE_METHOD))
        .sslKeyStorePath(ConfigUtils.parseOptional(props, SSL_KEYSTORE_PATH, Paths::get))
        .sslKeyStorePassword(ConfigUtils.parseOptional(props, SSL_KEYSTORE_PASSWORD))
        .sslKeyStoreAlias(ConfigUtils.parseOptional(props, SSL_KEYSTORE_ALIAS))
        .sslProtocols(ConfigUtils.parseList(props, SSL_PROTOCOLS, ","))
        .sslCipherSuites(ConfigUtils.parseList(props, SSL_CIPHER_SUITES, ","));
  }
}
