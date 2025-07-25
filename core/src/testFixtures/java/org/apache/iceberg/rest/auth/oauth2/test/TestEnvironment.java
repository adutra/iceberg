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
package org.apache.iceberg.rest.auth.oauth2.test;

import com.google.errorprone.annotations.MustBeClosed;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Manager;
import org.apache.iceberg.rest.auth.oauth2.client.ImmutableOAuth2ClientRuntime;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2ClientRuntime;
import org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtils;
import org.apache.iceberg.rest.auth.oauth2.config.DeviceCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableAuthorizationCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableBasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableDeviceCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableHttpClientConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableResourceOwnerConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableTokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableTokenRefreshConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ResourceOwnerConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig;
import org.apache.iceberg.rest.auth.oauth2.flow.FlowFactory;
import org.apache.iceberg.rest.auth.oauth2.http.HttpClientType;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment.Builder;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableAuthorizationCodeExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableClientCredentialsExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableConfigEndpointExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableDeviceCodeExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableErrorExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableLoadTableEndpointExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableMetadataDiscoveryExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutablePasswordExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableRefreshTokenExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableTokenExchangeExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.user.InteractiveUserEmulator;
import org.apache.iceberg.rest.auth.oauth2.test.user.UserBehavior;
import org.apache.iceberg.rest.auth.oauth2.test.user.UserEmulator;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;
import org.mockserver.integration.ClientAndServer;

@Value.Immutable(copy = false)
@SuppressWarnings("resource")
public abstract class TestEnvironment implements AutoCloseable {

  public static final Instant NOW = Instant.parse("2025-01-01T00:00:00Z");

  public static final int ACCESS_TOKEN_EXPIRES_IN_SECONDS = 3600;
  public static final int REFRESH_TOKEN_EXPIRES_IN_SECONDS = 86400;

  public static final ClientID CLIENT_ID1 = new ClientID("Client1");
  public static final ClientID CLIENT_ID2 = new ClientID("Client2");

  public static final Secret CLIENT_SECRET1 = new Secret("s3cr3t");
  public static final Secret CLIENT_SECRET2 = new Secret("sEcrEt");

  public static final String USERNAME = "Alice";
  public static final Secret PASSWORD = new Secret("s3cr3t");

  public static final Scope SCOPE1 = new Scope("catalog");
  public static final Scope SCOPE2 = new Scope("session");

  public static final TypelessAccessToken SUBJECT_TOKEN = new TypelessAccessToken("subject");
  public static final TypelessAccessToken ACTOR_TOKEN = new TypelessAccessToken("actor");

  public static final String WAREHOUSE = "warehouse1";
  public static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("namespace1", "table1");

  public static Builder builder() {
    return ImmutableTestEnvironment.builder();
  }

  // OAuth2 configuration

  @Value.Default
  public GrantType grantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public Optional<ClientID> clientId() {
    return Optional.of(CLIENT_ID1);
  }

  @Value.Default
  public Optional<Secret> clientSecret() {
    return Optional.of(CLIENT_SECRET1);
  }

  @Value.Default
  public ClientAuthenticationMethod clientAuthenticationMethod() {
    return ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
  }

  @Value.Default
  public Map<String, String> extraRequestParameters() {
    return Map.of("extra1", "value1");
  }

  public abstract Optional<TypelessAccessToken> token();

  @Value.Default
  public Scope scope() {
    return SCOPE1;
  }

  @Value.Default
  public Duration timeout() {
    return unitTest() ? Duration.ofSeconds(5) : BasicConfig.DEFAULT_TIMEOUT;
  }

  @Value.Default
  public String clientName() {
    return "iceberg-oauth2-client-" + System.nanoTime();
  }

  @Value.Default
  public boolean tokenRefreshEnabled() {
    return true;
  }

  @Value.Default
  public GrantType refreshGrantType() {
    // In tests, we use token exchange by default when the initial grant is client_credentials,
    // in order to test legacy behavior, but we use the more standard refresh_token grant for
    // all other initial grants.
    return grantType().equals(GrantType.CLIENT_CREDENTIALS)
        ? GrantType.TOKEN_EXCHANGE
        : GrantType.REFRESH_TOKEN;
  }

  @Value.Default
  public Duration accessTokenLifespan() {
    return Duration.ofSeconds(ACCESS_TOKEN_EXPIRES_IN_SECONDS);
  }

  @Value.Default
  public Duration refreshTokenLifespan() {
    return Duration.ofSeconds(REFRESH_TOKEN_EXPIRES_IN_SECONDS);
  }

  @Value.Default
  public ResourceOwnerConfig resourceOwnerConfig() {
    return ImmutableResourceOwnerConfig.builder().username(username()).password(password()).build();
  }

  @Value.Default
  public String username() {
    return USERNAME;
  }

  @Value.Default
  public Secret password() {
    return PASSWORD;
  }

  @Value.Default
  public boolean pkceEnabled() {
    return true;
  }

  @Value.Default
  public CodeChallengeMethod codeChallengeMethod() {
    return CodeChallengeMethod.S256;
  }

  public abstract Optional<URI> redirectUri();

  @Value.Default
  public boolean callbackHttps() {
    return false;
  }

  public abstract Optional<Path> sslKeyStorePath();

  public abstract Optional<String> sslKeyStorePassword();

  public abstract Optional<String> sslKeyStoreAlias();

  @Value.Default
  public Duration pollInterval() {
    return Duration.ofMillis(10);
  }

  @Value.Default
  public Optional<TypelessAccessToken> subjectToken() {
    return Optional.of(SUBJECT_TOKEN);
  }

  @Value.Default
  public TokenTypeURI subjectTokenType() {
    return TokenTypeURI.ACCESS_TOKEN;
  }

  @Value.Default
  public GrantType subjectGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public ClientID subjectClientId() {
    return CLIENT_ID2;
  }

  @Value.Default
  public ClientAuthenticationMethod subjectClientAuthenticationMethod() {
    return ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
  }

  @Value.Default
  public Secret subjectClientSecret() {
    return CLIENT_SECRET2;
  }

  @Value.Default
  public Scope subjectScope() {
    return SCOPE2;
  }

  @Value.Default
  public Optional<TypelessAccessToken> actorToken() {
    return Optional.of(ACTOR_TOKEN);
  }

  @Value.Default
  public TokenTypeURI actorTokenType() {
    return TokenTypeURI.ACCESS_TOKEN;
  }

  @Value.Default
  public GrantType actorGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public ClientID actorClientId() {
    return CLIENT_ID2;
  }

  @Value.Default
  public ClientAuthenticationMethod actorClientAuthenticationMethod() {
    return ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
  }

  @Value.Default
  public Secret actorClientSecret() {
    return CLIENT_SECRET2;
  }

  @Value.Default
  public Scope actorScope() {
    return SCOPE2;
  }

  @Value.Default
  public TokenTypeURI requestedTokenType() {
    return TokenTypeURI.ACCESS_TOKEN;
  }

  @Value.Default
  public List<Audience> audiences() {
    return List.of(new Audience("audience"));
  }

  @Value.Default
  public Optional<URI> resource() {
    return Optional.of(URI.create("urn:iceberg-oauth2-client:test:resource"));
  }

  public abstract Optional<JWSAlgorithm> jwsAlgorithm();

  public abstract Optional<Path> privateKey();

  @Value.Default
  public HttpClientType httpClientType() {
    return HttpClientType.DEFAULT;
  }

  @Value.Default
  public List<String> sslProtocols() {
    return List.of();
  }

  @Value.Default
  public List<String> sslCipherSuites() {
    return List.of();
  }

  @Value.Default
  public boolean sslTrustAll() {
    return false;
  }

  @Value.Default
  public boolean sslHostnameVerificationEnabled() {
    return true;
  }

  public abstract Optional<String> proxyHost();

  public abstract OptionalInt proxyPort();

  public abstract Optional<String> proxyUsername();

  public abstract Optional<String> proxyPassword();

  public abstract Optional<Path> sslTrustStorePath();

  public abstract Optional<String> sslTrustStorePassword();

  // General configuration

  @Value.Default
  public boolean unitTest() {
    return true;
  }

  @Value.Lazy
  public Optional<ClientAndServer> mockServer() {
    return unitTest() ? Optional.of(ClientAndServer.startClientAndServer()) : Optional.empty();
  }

  @Value.Default
  public ScheduledExecutorService executor() {
    return ThreadPools.newScheduledPool(clientName() + "-refresh", executorPoolSize());
  }

  @Value.Default
  public int executorPoolSize() {
    return 1;
  }

  @Value.Default
  public boolean discoveryEnabled() {
    return true;
  }

  @Value.Default
  public boolean returnRefreshTokens() {
    return !grantType().equals(GrantType.CLIENT_CREDENTIALS);
  }

  @Value.Default
  public boolean returnRefreshTokenLifespan() {
    return true;
  }

  @Value.Default
  public boolean includeDeviceAuthEndpointInDiscoveryMetadata() {
    return true;
  }

  @Value.Default
  public boolean createDefaultExpectations() {
    return unitTest();
  }

  @Value.Default
  public boolean ssl() {
    return false;
  }

  // URLs, endpoints and paths

  @Value.Default
  public URI serverRootUrl() {
    // Note: the default value is for unit tests with MockServer only;
    // integration tests must provide the server root URL explicitly
    return mockServer()
        .map(
            server ->
                URI.create((ssl() ? "https" : "http") + "://localhost:" + server.getLocalPort()))
        .orElseThrow();
  }

  @Value.Default
  public String authorizationServerContextPath() {
    return "/realms/master/";
  }

  @Value.Default
  public String catalogServerContextPath() {
    return "/api/catalog/";
  }

  @Value.Default
  public URI authorizationServerUrl() {
    return serverRootUrl().resolve(authorizationServerContextPath());
  }

  @Value.Default
  public URI catalogServerUrl() {
    return serverRootUrl().resolve(catalogServerContextPath());
  }

  @Value.Default
  public URI tokenEndpoint() {
    return authorizationServerUrl().resolve("protocol/openid-connect/token");
  }

  @Value.Default
  public URI authorizationEndpoint() {
    return authorizationServerUrl().resolve("protocol/openid-connect/auth");
  }

  @Value.Default
  public URI deviceAuthorizationEndpoint() {
    return authorizationServerUrl().resolve("protocol/openid-connect/device-auth");
  }

  @Value.Default
  public URI deviceVerificationEndpoint() {
    return authorizationServerUrl().resolve("device");
  }

  @Value.Default
  public String wellKnownPath() {
    return ".well-known/openid-configuration";
  }

  @Value.Default
  public URI discoveryEndpoint() {
    return authorizationServerUrl().resolve(wellKnownPath());
  }

  @Value.Default
  public URI configEndpoint() {
    return catalogServerUrl().resolve(ResourcePaths.config());
  }

  @Value.Default
  public URI loadTableEndpoint() {
    return catalogServerUrl()
        .resolve(
            ResourcePaths.forCatalogProperties(Map.of("prefix", WAREHOUSE))
                .table(TABLE_IDENTIFIER));
  }

  // REST Catalog configuration

  @Value.Default
  public Map<String, String> catalogProperties() {

    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, catalogServerUrl().toString())
            .put("prefix", WAREHOUSE)
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .put(AuthProperties.AUTH_TYPE, OAuth2Manager.class.getName());

    // Note: we don't include all possible OAuth2 properties here, only the ones that are
    // relevant for catalog tests.

    builder
        .put(BasicConfig.PREFIX + BasicConfig.GRANT_TYPE, grantType().toString())
        .put(BasicConfig.PREFIX + BasicConfig.SCOPE, scope().toString())
        .put(BasicConfig.PREFIX + BasicConfig.CLIENT_AUTH, clientAuthenticationMethod().toString());
    token().ifPresent(t -> builder.put(BasicConfig.PREFIX + BasicConfig.TOKEN, t.getValue()));
    clientId()
        .ifPresent(id -> builder.put(BasicConfig.PREFIX + BasicConfig.CLIENT_ID, id.getValue()));
    if (ConfigUtils.requiresClientSecret(clientAuthenticationMethod())) {
      clientSecret()
          .ifPresent(
              secret ->
                  builder.put(BasicConfig.PREFIX + BasicConfig.CLIENT_SECRET, secret.getValue()));
    }
    extraRequestParameters()
        .forEach((k, v) -> builder.put(BasicConfig.PREFIX + BasicConfig.EXTRA_PARAMS + '.' + k, v));
    if (discoveryEnabled()) {
      builder.put(BasicConfig.PREFIX + BasicConfig.ISSUER_URL, authorizationServerUrl().toString());
    } else {
      builder.put(BasicConfig.PREFIX + BasicConfig.TOKEN_ENDPOINT, tokenEndpoint().toString());
    }

    if (grantType().equals(GrantType.PASSWORD)) {
      builder
          .put(ResourceOwnerConfig.PREFIX + ResourceOwnerConfig.USERNAME, username())
          .put(ResourceOwnerConfig.PREFIX + ResourceOwnerConfig.PASSWORD, password().getValue());
    } else if (grantType().equals(GrantType.TOKEN_EXCHANGE)) {
      builder
          .put(
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN_TYPE,
              subjectTokenType().toString())
          .put(
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.ACTOR_TOKEN_TYPE,
              actorTokenType().toString())
          .put(
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.REQUESTED_TOKEN_TYPE,
              requestedTokenType().toString());
      subjectToken()
          .ifPresent(
              t ->
                  builder.put(
                      TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN,
                      t.getValue()));
      actorToken()
          .ifPresent(
              t ->
                  builder.put(
                      TokenExchangeConfig.PREFIX + TokenExchangeConfig.ACTOR_TOKEN, t.getValue()));
      resource()
          .ifPresent(
              r ->
                  builder.put(
                      TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE, r.toString()));
      if (!audiences().isEmpty()) {
        builder.put(
            TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES,
            audiences().stream().map(Audience::getValue).collect(Collectors.joining(",")));
      }
    }

    if (ConfigUtils.requiresJwsAlgorithm(clientAuthenticationMethod())) {
      jwsAlgorithm()
          .ifPresent(
              a ->
                  builder.put(
                      ClientAssertionConfig.PREFIX + ClientAssertionConfig.ALGORITHM, a.getName()));
      privateKey()
          .ifPresent(
              p ->
                  builder.put(
                      ClientAssertionConfig.PREFIX + ClientAssertionConfig.PRIVATE_KEY,
                      p.toString()));
    }

    return builder.build();
  }

  @Value.Default
  public SessionContext sessionContext() {
    return SessionContext.createEmpty();
  }

  @Value.Default
  public Map<String, String> tableProperties() {
    return Map.of();
  }

  // OAuth2 Configuration objects

  @Value.Default
  public BasicConfig basicConfig() {
    ImmutableBasicConfig.Builder builder =
        ImmutableBasicConfig.builder()
            .grantType(grantType())
            .token(token())
            .clientId(clientId())
            .clientAuthenticationMethod(clientAuthenticationMethod())
            .clientSecret(
                ConfigUtils.requiresClientSecret(clientAuthenticationMethod())
                    ? clientSecret()
                    : Optional.empty())
            .scope(scope())
            .extraRequestParameters(extraRequestParameters())
            .timeout(timeout())
            .minTimeout(timeout())
            .clientName(clientName());
    if (discoveryEnabled()) {
      builder.issuerUrl(authorizationServerUrl());
    } else {
      builder.tokenEndpoint(tokenEndpoint());
    }

    return builder.build();
  }

  @Value.Default
  public TokenRefreshConfig tokenRefreshConfig() {
    return ImmutableTokenRefreshConfig.builder()
        .enabled(tokenRefreshEnabled())
        .grantType(refreshGrantType())
        .accessTokenLifespan(accessTokenLifespan())
        .safetyMargin(Duration.ofSeconds(5))
        .idleTimeout(Duration.ofSeconds(5))
        .minAccessTokenLifespan(accessTokenLifespan())
        .minRefreshDelay(Duration.ofSeconds(5))
        .minIdleTimeout(Duration.ofSeconds(5))
        .build();
  }

  @Value.Default
  public AuthorizationCodeConfig authorizationCodeConfig() {
    ImmutableAuthorizationCodeConfig.Builder builder =
        ImmutableAuthorizationCodeConfig.builder()
            .pkceEnabled(pkceEnabled())
            .codeChallengeMethod(codeChallengeMethod())
            .callbackHttps(callbackHttps());
    if (!discoveryEnabled()) {
      builder.authorizationEndpoint(authorizationEndpoint());
    }

    redirectUri().ifPresent(builder::redirectUri);
    if (callbackHttps()) {
      sslKeyStorePath().ifPresent(builder::sslKeyStorePath);
      sslKeyStorePassword().ifPresent(builder::sslKeyStorePassword);
      sslKeyStoreAlias().ifPresent(builder::sslKeyStoreAlias);
    }

    return builder.build();
  }

  @Value.Default
  public DeviceCodeConfig deviceCodeConfig() {
    ImmutableDeviceCodeConfig.Builder builder =
        ImmutableDeviceCodeConfig.builder()
            .pollInterval(pollInterval())
            .minPollInterval(pollInterval())
            .ignoreServerPollInterval(true);
    if (!discoveryEnabled()) {
      builder.deviceAuthorizationEndpoint(deviceAuthorizationEndpoint());
    }

    return builder.build();
  }

  @Value.Default
  public TokenExchangeConfig tokenExchangeConfig() {
    return ImmutableTokenExchangeConfig.builder()
        .subjectToken(subjectToken())
        .subjectTokenType(subjectTokenType())
        .subjectTokenConfig(subjectTokenConfig())
        .actorToken(actorToken())
        .actorTokenType(actorTokenType())
        .actorTokenConfig(actorTokenConfig())
        .requestedTokenType(requestedTokenType())
        .audiences(audiences())
        .resource(resource())
        .build();
  }

  @Value.Default
  public OAuth2Config subjectTokenConfig() {
    BasicConfig basicConfig =
        ImmutableBasicConfig.builder()
            .tokenEndpoint(tokenEndpoint())
            .issuerUrl(authorizationServerUrl())
            .grantType(subjectGrantType())
            .clientId(subjectClientId())
            .clientAuthenticationMethod(subjectClientAuthenticationMethod())
            .clientSecret(
                ConfigUtils.requiresClientSecret(subjectClientAuthenticationMethod())
                    ? Optional.of(subjectClientSecret())
                    : Optional.empty())
            .scope(subjectScope())
            .extraRequestParameters(Map.of("extra2", "value2"))
            .timeout(timeout())
            .minTimeout(timeout())
            .build();
    return ImmutableOAuth2Config.builder()
        .basicConfig(basicConfig)
        .tokenRefreshConfig(tokenRefreshConfig())
        .httpClientConfig(httpClientConfig())
        .clientAssertionConfig(clientAssertionConfig())
        .resourceOwnerConfig(resourceOwnerConfig())
        .authorizationCodeConfig(authorizationCodeConfig())
        .deviceCodeConfig(deviceCodeConfig())
        .build();
  }

  @Value.Default
  public OAuth2Config actorTokenConfig() {
    BasicConfig basicConfig =
        ImmutableBasicConfig.builder()
            .tokenEndpoint(tokenEndpoint())
            .issuerUrl(authorizationServerUrl())
            .grantType(actorGrantType())
            .clientId(actorClientId())
            .clientAuthenticationMethod(actorClientAuthenticationMethod())
            .clientSecret(
                ConfigUtils.requiresClientSecret(actorClientAuthenticationMethod())
                    ? Optional.of(actorClientSecret())
                    : Optional.empty())
            .scope(actorScope())
            .extraRequestParameters(Map.of("extra2", "value2"))
            .timeout(timeout())
            .minTimeout(timeout())
            .build();
    return ImmutableOAuth2Config.builder()
        .basicConfig(basicConfig)
        .tokenRefreshConfig(tokenRefreshConfig())
        .httpClientConfig(httpClientConfig())
        .clientAssertionConfig(clientAssertionConfig())
        .resourceOwnerConfig(resourceOwnerConfig())
        .authorizationCodeConfig(authorizationCodeConfig())
        .deviceCodeConfig(deviceCodeConfig())
        .build();
  }

  @Value.Default
  public ClientAssertionConfig clientAssertionConfig() {
    return ImmutableClientAssertionConfig.builder()
        .algorithm(jwsAlgorithm())
        .privateKey(privateKey())
        .build();
  }

  @Value.Default
  public HttpClientConfig httpClientConfig() {
    ImmutableHttpClientConfig.Builder builder =
        ImmutableHttpClientConfig.builder()
            .clientType(httpClientType())
            .sslTrustAll(sslTrustAll())
            .sslHostnameVerificationEnabled(sslHostnameVerificationEnabled())
            .sslProtocols(sslProtocols())
            .sslCipherSuites(sslCipherSuites());
    proxyHost().ifPresent(builder::proxyHost);
    proxyPort().ifPresent(builder::proxyPort);
    proxyUsername().ifPresent(builder::proxyUsername);
    proxyPassword().ifPresent(builder::proxyPassword);
    sslTrustStorePath().ifPresent(builder::sslTrustStorePath);
    sslTrustStorePassword().ifPresent(builder::sslTrustStorePassword);
    return builder.build();
  }

  @Value.Default
  public OAuth2Config config() {
    return ImmutableOAuth2Config.builder()
        .basicConfig(basicConfig())
        .tokenRefreshConfig(tokenRefreshConfig())
        .resourceOwnerConfig(resourceOwnerConfig())
        .authorizationCodeConfig(authorizationCodeConfig())
        .deviceCodeConfig(deviceCodeConfig())
        .tokenExchangeConfig(tokenExchangeConfig())
        .clientAssertionConfig(clientAssertionConfig())
        .httpClientConfig(httpClientConfig())
        .build();
  }

  // User Emulation

  @Value.Default
  public boolean forceInactiveUser() {
    return false;
  }

  @Value.Default
  public UserBehavior userBehavior() {
    return unitTest() ? UserBehavior.UNIT_TESTS : UserBehavior.INTEGRATION_TESTS;
  }

  @Value.Default
  public UserEmulator userEmulator() {
    if (forceInactiveUser()) {
      return UserEmulator.INACTIVE;
    } else {
      GrantType mainGrant = grantType();
      GrantType subjectGrant = subjectGrantType();
      GrantType actorGrant = actorGrantType();
      if (ConfigUtils.requiresUserInteraction(mainGrant)
          || ConfigUtils.requiresUserInteraction(subjectGrant)
          || ConfigUtils.requiresUserInteraction(actorGrant)) {
        return new InteractiveUserEmulator(userBehavior(), userSslContext());
      }
    }

    return UserEmulator.INACTIVE;
  }

  @Value.Default
  public SSLContext userSslContext() {
    try {
      return SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  // Client Runtime

  @Value.Default
  public OAuth2ClientRuntime clientRuntime() {
    return ImmutableOAuth2ClientRuntime.builder()
        .executor(executor())
        .clock(clock())
        .console(console())
        .build();
  }

  @Value.Default
  public Clock clock() {
    return unitTest() ? new TestClock(NOW) : Clock.systemUTC();
  }

  @Value.Derived
  public PrintStream console() {
    return userEmulator().console();
  }

  // Lifecycle methods

  @Value.Check
  public void initialize() {
    if (createDefaultExpectations()) {
      createExpectations();
    }
  }

  public void reset() {
    mockServer().ifPresent(ClientAndServer::reset);
  }

  @Override
  public void close() {
    userEmulator().close();
    mockServer().ifPresent(ClientAndServer::close);
    try {
      executor().shutdown();
      if (!executor().awaitTermination(10, TimeUnit.SECONDS)) {
        executor().shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // Factory methods

  public HTTPClient.Builder newIcebergRestClientBuilder(Map<String, String> properties) {
    return HTTPClient.builder(properties)
        .uri(catalogServerUrl())
        .withAuthSession(AuthSession.EMPTY);
  }

  @MustBeClosed
  public RESTCatalog newCatalog() {
    RESTCatalog catalog =
        new RESTCatalog(sessionContext(), config -> newIcebergRestClientBuilder(config).build());
    UserEmulator user = userEmulator();
    user.addErrorListener(
        e -> {
          try {
            catalog.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        });
    catalog.initialize("catalog-" + System.nanoTime(), catalogProperties());
    return catalog;
  }

  @MustBeClosed
  public FlowFactory newFlowFactory() {
    FlowFactory flowFactory = FlowFactory.create(config(), clientRuntime());
    userEmulator().addErrorListener(e -> flowFactory.close());
    return flowFactory;
  }

  @MustBeClosed
  public OAuth2Client newClient() {
    OAuth2Client client = new OAuth2Client(config(), clientRuntime());
    userEmulator().addErrorListener(e -> client.close());
    return client;
  }

  // MockServer Expectations

  public void createExpectations() {
    createInitialGrantExpectations();
    createRefreshTokenExpectations();
    createCatalogExpectations();
    createMetadataDiscoveryExpectations();
    createErrorExpectations();
  }

  public void createInitialGrantExpectations() {
    Set<GrantType> grantTypes = ImmutableSet.of(grantType(), subjectGrantType(), actorGrantType());
    for (GrantType grantType : grantTypes) {
      if (grantType.equals(GrantType.CLIENT_CREDENTIALS)) {
        ImmutableClientCredentialsExpectation.of(this).create();
      } else if (grantType.equals(GrantType.PASSWORD)) {
        ImmutablePasswordExpectation.of(this).create();
      } else if (grantType.equals(GrantType.AUTHORIZATION_CODE)) {
        ImmutableAuthorizationCodeExpectation.of(this).create();
      } else if (grantType.equals(GrantType.DEVICE_CODE)) {
        ImmutableDeviceCodeExpectation.of(this).create();
      } else if (grantType.equals(GrantType.TOKEN_EXCHANGE)) {
        ImmutableTokenExchangeExpectation.of(this).create();
      }
    }
  }

  public void createRefreshTokenExpectations() {
    if (tokenRefreshEnabled()) {
      ImmutableRefreshTokenExpectation.of(this).create();
    }
  }

  public void createCatalogExpectations() {
    ImmutableConfigEndpointExpectation.of(this).create();
    ImmutableLoadTableEndpointExpectation.of(this).create();
  }

  public void createMetadataDiscoveryExpectations() {
    if (discoveryEnabled()) {
      ImmutableMetadataDiscoveryExpectation.of(this).create();
    }
  }

  public void createErrorExpectations() {
    ImmutableErrorExpectation.of(this).create();
  }

  // Prevent generation of equals(), hashCode() and toString() as this class is big
  // and the generated methods are not useful.

  @Override
  public final int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public final boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public final String toString() {
    return "TestEnvironment";
  }
}
