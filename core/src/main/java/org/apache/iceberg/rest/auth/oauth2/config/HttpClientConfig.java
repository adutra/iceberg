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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.http.HttpClientType;
import org.immutables.value.Value;

/** Configuration properties for HTTP clients. */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface HttpClientConfig {

  String GROUP_NAME = "http";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String CLIENT_TYPE = "client-type";
  String READ_TIMEOUT = "read-timeout";
  String CONNECT_TIMEOUT = "connect-timeout";
  String HEADERS = "headers";
  String COMPRESSION_ENABLED = "compression.enabled";
  String SSL_PROTOCOLS = "ssl.protocols";
  String SSL_CIPHER_SUITES = "ssl.cipher-suites";
  String SSL_HOSTNAME_VERIFICATION_ENABLED = "ssl.hostname-verification.enabled";
  String SSL_TRUST_ALL = "ssl.trust-all";
  String SSL_TRUSTSTORE_PATH = "ssl.trust-store.path";
  String SSL_TRUSTSTORE_PASSWORD = "ssl.trust-store.password";
  String PROXY_HOST = "proxy.host";
  String PROXY_PORT = "proxy.port";
  String PROXY_USERNAME = "proxy.username";
  String PROXY_PASSWORD = "proxy.password";

  Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
  Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(10);

  /**
   * The type of HTTP client to use for making HTTP requests to the OAuth2 server. Valid values are:
   *
   * <ul>
   *   <li>{@link HttpClientType#DEFAULT}: uses the built-in URLConnection-based client provided by
   *       the underlying OAuth2 library.
   *   <li>{@link HttpClientType#APACHE}: uses the Apache HttpClient library, provided by Iceberg's
   *       runtime.
   * </ul>
   *
   * <p>Optional, defaults to {@code default}.
   */
  @ConfigOption(CLIENT_TYPE)
  @Value.Default
  default HttpClientType clientType() {
    return HttpClientType.DEFAULT;
  }

  /**
   * The read timeout for HTTP requests. Optional, defaults to {@link #DEFAULT_READ_TIMEOUT}. Must
   * be a valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(READ_TIMEOUT)
  @Value.Default
  default Duration readTimeout() {
    return DEFAULT_READ_TIMEOUT;
  }

  /**
   * The connection timeout for HTTP requests. Optional, defaults to {@link
   * #DEFAULT_CONNECTION_TIMEOUT}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(CONNECT_TIMEOUT)
  @Value.Default
  default Duration connectionTimeout() {
    return DEFAULT_CONNECTION_TIMEOUT;
  }

  /**
   * HTTP headers to include in each HTTP request. This is a prefix property, and multiple values
   * can be set, each with a different key and value.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(value = HEADERS, prefixMap = true)
  Map<String, String> headers();

  /**
   * Whether to enable compression for HTTP requests. Optional, defaults to {@code true}.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(COMPRESSION_ENABLED)
  @Value.Default
  default boolean compressionEnabled() {
    return true;
  }

  /**
   * A comma-separated list of SSL protocols to use for HTTPS requests. Optional, defaults to the
   * system protocols.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(SSL_PROTOCOLS)
  List<String> sslProtocols();

  /**
   * A comma-separated list of SSL cipher suites to use for HTTPS requests. Optional, defaults to
   * the system cipher suites.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(SSL_CIPHER_SUITES)
  List<String> sslCipherSuites();

  /**
   * Whether to enable SSL hostname verification for HTTPS requests.
   *
   * <p>WARNING: Disabling hostname verification is a security risk and should only be used for
   * testing purposes.
   *
   * <p>Optional, defaults to {@code true}.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(SSL_HOSTNAME_VERIFICATION_ENABLED)
  @Value.Default
  default boolean sslHostnameVerificationEnabled() {
    return true;
  }

  /**
   * Whether to trust all SSL certificates for HTTPS requests.
   *
   * <p>WARNING: Trusting all SSL certificates is a security risk and should only be used for
   * testing purposes.
   *
   * <p>Optional, defaults to {@code false}.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(SSL_TRUST_ALL)
  @Value.Default
  default boolean sslTrustAll() {
    return false;
  }

  /**
   * Path to the trust store to use for HTTPS requests. Optional, defaults to the system trust
   * store.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(SSL_TRUSTSTORE_PATH)
  Optional<Path> sslTrustStorePath();

  /**
   * Password for the trust store to use for HTTPS requests. Optional, defaults to no password.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}, or if {@link #SSL_TRUSTSTORE_PATH} is not set.
   */
  @ConfigOption(SSL_TRUSTSTORE_PASSWORD)
  @Value.Redacted
  Optional<String> sslTrustStorePassword();

  /**
   * Proxy host to use for HTTP requests. Optional, defaults to no proxy. If set, the proxy port
   * must also be set.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(PROXY_HOST)
  Optional<String> proxyHost();

  /**
   * Proxy port to use for HTTP requests. Optional, defaults to no proxy. If set, the proxy host
   * must also be set.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(PROXY_PORT)
  OptionalInt proxyPort();

  /**
   * Proxy username to use for HTTP requests. Optional, defaults to no authentication. If set, the
   * proxy password must also be set.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(PROXY_USERNAME)
  Optional<String> proxyUsername();

  /**
   * Proxy password to use for HTTP requests. Optional, defaults to no authentication. If set, the
   * proxy username must also be set.
   *
   * <p>This setting is ignored when the {@linkplain #CLIENT_TYPE client type} is set to {@code
   * default}.
   */
  @ConfigOption(PROXY_PASSWORD)
  @Value.Redacted
  Optional<String> proxyPassword();

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    if (sslTrustStorePath().isPresent()) {
      validator.check(
          Files.isReadable(sslTrustStorePath().get()),
          PREFIX + SSL_TRUSTSTORE_PATH,
          "http: SSL truststore path '%s' is not a file or is not readable",
          sslTrustStorePath().get());
    }

    validator.validate();
  }

  static ImmutableHttpClientConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    return ImmutableHttpClientConfig.builder()
        .clientType(
            ConfigUtils.parseOptional(props, CLIENT_TYPE, HttpClientType::fromString)
                .orElse(HttpClientType.DEFAULT))
        .readTimeout(
            ConfigUtils.parseOptional(props, READ_TIMEOUT, Duration::parse)
                .orElse(DEFAULT_READ_TIMEOUT))
        .connectionTimeout(
            ConfigUtils.parseOptional(props, CONNECT_TIMEOUT, Duration::parse)
                .orElse(DEFAULT_CONNECTION_TIMEOUT))
        .headers(RESTUtil.extractPrefixMap(props, HEADERS + '.'))
        .compressionEnabled(
            ConfigUtils.parseOptional(props, COMPRESSION_ENABLED, Boolean::parseBoolean)
                .orElse(true))
        .sslProtocols(ConfigUtils.parseList(props, SSL_PROTOCOLS, ","))
        .sslCipherSuites(ConfigUtils.parseList(props, SSL_CIPHER_SUITES, ","))
        .sslHostnameVerificationEnabled(
            ConfigUtils.parseOptional(
                    props, SSL_HOSTNAME_VERIFICATION_ENABLED, Boolean::parseBoolean)
                .orElse(true))
        .sslTrustAll(
            ConfigUtils.parseOptional(props, SSL_TRUST_ALL, Boolean::parseBoolean).orElse(false))
        .sslTrustStorePath(ConfigUtils.parseOptional(props, SSL_TRUSTSTORE_PATH, Paths::get))
        .sslTrustStorePassword(ConfigUtils.parseOptional(props, SSL_TRUSTSTORE_PASSWORD))
        .proxyHost(ConfigUtils.parseOptional(props, PROXY_HOST))
        .proxyPort(ConfigUtils.parseOptionalInt(props, PROXY_PORT))
        .proxyUsername(ConfigUtils.parseOptional(props, PROXY_USERNAME))
        .proxyPassword(ConfigUtils.parseOptional(props, PROXY_PASSWORD));
  }
}
