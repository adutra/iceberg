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

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.IcebergCoreHooks;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Manager;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.agent.OAuth2Agent;
import org.apache.iceberg.rest.auth.oauth2.agent.OAuth2AgentSpec;
import org.apache.iceberg.rest.auth.oauth2.auth.ClientAuthentication;
import org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtils;
import org.apache.iceberg.rest.auth.oauth2.config.DeviceCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.PkceTransformation;
import org.apache.iceberg.rest.auth.oauth2.config.ResourceOwnerPasswordConfig;
import org.apache.iceberg.rest.auth.oauth2.config.RuntimeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig;
import org.apache.iceberg.rest.auth.oauth2.endpoint.EndpointProvider;
import org.apache.iceberg.rest.auth.oauth2.endpoint.EndpointProviderFactory;
import org.apache.iceberg.rest.auth.oauth2.flow.FlowFactory;
import org.apache.iceberg.rest.auth.oauth2.flow.FlowUtils;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
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
import org.apache.iceberg.rest.auth.oauth2.test.server.HttpServer;
import org.apache.iceberg.rest.auth.oauth2.test.server.InactiveHttpServer;
import org.apache.iceberg.rest.auth.oauth2.test.server.MockHttpServer;
import org.apache.iceberg.rest.auth.oauth2.test.user.InteractiveUserEmulator;
import org.apache.iceberg.rest.auth.oauth2.test.user.UserBehavior;
import org.apache.iceberg.rest.auth.oauth2.test.user.UserEmulator;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;

/**
 * A test environment for OAuth2-based authentication, providing a mock HTTP server, HTTP client,
 * and various configurations for testing OAuth2 flows.
 *
 * <p>A test environment is AutoCloseable and is meant to be used in a try-with-resources block to
 * ensure proper cleanup of resources such as the HTTP client, HTTP server, and executor service.
 */
@Value.Immutable
@OAuth2ImmutableStyle
@SuppressWarnings("resource")
public abstract class TestEnvironment implements AutoCloseable {

  static {
    // Ensure OAuth2 serializers are registered early
    IcebergCoreHooks.installOAuth2Serializers();
  }

  public static ImmutableTestEnvironment.Builder builder() {
    return ImmutableTestEnvironment.builder();
  }

  @Value.Check
  public void validate() {
    Preconditions.checkArgument(grantType().initial(), "Grant type must be initial");
    if (createDefaultExpectations()) {
      createExpectations();
    }
  }

  @Value.Default
  public GrantType grantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public boolean unitTest() {
    return true;
  }

  @Value.Default
  public boolean privateClient() {
    return true;
  }

  @Value.Default
  public boolean discoveryEnabled() {
    return true;
  }

  @Value.Default
  public boolean returnRefreshTokens() {
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

  @Value.Lazy
  public HttpServer server() {
    return unitTest() ? new MockHttpServer() : InactiveHttpServer.INSTANCE;
  }

  @Value.Default
  public HTTPClient httpClient() {
    return newHttpClientBuilder(Map.of()).build();
  }

  public HTTPClient.Builder newHttpClientBuilder(Map<String, String> properties) {
    return HTTPClient.builder(properties)
        .uri(catalogServerUrl())
        .withAuthSession(AuthSession.EMPTY);
  }

  @Value.Default
  public ScheduledExecutorService executor() {
    return ThreadPools.newScheduledPool(agentName() + "-refresh", executorPoolSize());
  }

  @Value.Default
  public int executorPoolSize() {
    return 1;
  }

  @Value.Lazy
  public EndpointProvider endpointProvider() {
    return EndpointProviderFactory.createEndpointProvider(agentSpec(), this::httpClient);
  }

  public void reset() {
    server().reset();
  }

  @Override
  public void close() {
    user().close();

    try {
      httpClient().close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    try {
      executor().shutdown();
      if (!executor().awaitTermination(10, TimeUnit.SECONDS)) {
        executor().shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    server().close();
  }

  @Value.Default
  public URI serverRootUrl() {
    // Note: the default value is for unit tests; integration tests must provide the server root URL
    // to avoid circular dependencies when creating the TestEnvironment instance
    return server().rootUrl();
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
  public URI discoveryEndpoint() {
    return authorizationServerUrl().resolve(wellKnownPath());
  }

  @Value.Default
  public String wellKnownPath() {
    return EndpointProvider.WELL_KNOWN_PATHS.get(0);
  }

  @Value.Default
  public URI configEndpoint() {
    return catalogServerUrl().resolve(ResourcePaths.config());
  }

  @Value.Default
  public URI loadTableEndpoint() {
    return catalogServerUrl()
        .resolve(
            ResourcePaths.forCatalogProperties(catalogProperties())
                .table(TestConstants.TABLE_IDENTIFIER));
  }

  @Value.Default
  public OAuth2AgentSpec agentSpec() {
    return OAuth2AgentSpec.builder()
        .basicConfig(basicConfig())
        .resourceOwnerPasswordConfig(resourceOwnerConfig())
        .authorizationCodeConfig(authorizationCodeConfig())
        .deviceCodeConfig(deviceCodeConfig())
        .tokenRefreshConfig(tokenRefreshConfig())
        .tokenExchangeConfig(tokenExchangeConfig())
        .authorizationCodeConfig(authorizationCodeConfig())
        .runtimeConfig(runtimeConfig())
        .build();
  }

  @Value.Default
  public BasicConfig basicConfig() {
    BasicConfig.Builder builder =
        BasicConfig.builder()
            .scopes(scopes())
            .extraRequestParameters(Map.of("extra1", "value1"))
            .grantType(grantType())
            .minTimeout(timeout())
            .timeout(timeout())
            .clientId(clientId());

    if (privateClient()) {
      builder.clientSecret(clientSecret());
    }

    clientAuthentication().ifPresent(builder::clientAuthentication);
    if (discoveryEnabled()) {
      builder.issuerUrl(authorizationServerUrl());
    } else {
      builder.tokenEndpoint(tokenEndpoint());
    }

    return builder.build();
  }

  @Value.Default
  public String clientId() {
    return TestConstants.CLIENT_ID1;
  }

  @Value.Default
  public String clientSecret() {
    return TestConstants.CLIENT_SECRET1;
  }

  public abstract Optional<ClientAuthentication> clientAuthentication();

  @Value.Default
  public List<String> scopes() {
    return List.of(TestConstants.SCOPE1);
  }

  @Value.Default
  public Duration timeout() {
    return Duration.ofSeconds(5);
  }

  @Value.Default
  public TokenRefreshConfig tokenRefreshConfig() {
    return TokenRefreshConfig.builder()
        .enabled(tokenRefreshEnabled())
        .accessTokenLifespan(accessTokenLifespan())
        .minAccessTokenLifespan(accessTokenLifespan())
        // safety margin and idle timeout are tailored for integration tests
        .safetyMargin(Duration.ofSeconds(5))
        .minRefreshDelay(Duration.ofSeconds(5))
        .idleTimeout(Duration.ofSeconds(5))
        .minIdleTimeout(Duration.ofSeconds(5))
        .build();
  }

  @Value.Default
  public boolean tokenRefreshEnabled() {
    return true;
  }

  @Value.Default
  public Duration accessTokenLifespan() {
    return TestConstants.ACCESS_TOKEN_LIFESPAN;
  }

  @Value.Default
  public Duration refreshTokenLifespan() {
    return TestConstants.REFRESH_TOKEN_LIFESPAN;
  }

  @Value.Default
  public ResourceOwnerPasswordConfig resourceOwnerConfig() {
    return ResourceOwnerPasswordConfig.builder()
        .username(TestConstants.USERNAME)
        .password(password())
        .build();
  }

  @Value.Default
  public String password() {
    return TestConstants.PASSWORD;
  }

  @Value.Default
  public AuthorizationCodeConfig authorizationCodeConfig() {
    AuthorizationCodeConfig.Builder builder =
        AuthorizationCodeConfig.builder()
            .pkceEnabled(pkceEnabled())
            .pkceTransformation(pkceTransformation());
    if (!discoveryEnabled()) {
      builder.authorizationEndpoint(authorizationEndpoint());
    }

    return builder.build();
  }

  @Value.Default
  public boolean pkceEnabled() {
    return true;
  }

  @Value.Default
  public PkceTransformation pkceTransformation() {
    return PkceTransformation.S256;
  }

  @Value.Default
  public DeviceCodeConfig deviceCodeConfig() {
    DeviceCodeConfig.Builder builder =
        DeviceCodeConfig.builder()
            .ignoreServerPollInterval(unitTest())
            .minPollInterval(Duration.ofMillis(10))
            .pollInterval(Duration.ofMillis(10));
    if (!discoveryEnabled()) {
      builder.deviceAuthorizationEndpoint(deviceAuthorizationEndpoint());
    }

    return builder.build();
  }

  @Value.Default
  public TokenExchangeConfig tokenExchangeConfig() {
    TokenExchangeConfig.Builder builder =
        TokenExchangeConfig.builder()
            .subjectTokenType(subjectTokenType())
            .actorTokenType(actorTokenType())
            .subjectTokenConfig(subjectTokenConfig())
            .actorTokenConfig(actorTokenConfig())
            .requestedTokenType(requestedTokenType());
    if (subjectToken() != null) {
      builder.subjectToken(subjectToken());
    }

    if (actorToken() != null) {
      builder.actorToken(actorToken());
    }

    if (audience() != null) {
      builder.audience(audience());
    }

    if (resource() != null) {
      builder.resource(resource());
    }

    return builder.build();
  }

  @Value.Default
  @Nullable
  public String subjectToken() {
    return TestConstants.SUBJECT_TOKEN;
  }

  @Value.Default
  public URI subjectTokenType() {
    return TestConstants.SUBJECT_TOKEN_TYPE;
  }

  @Value.Default
  public GrantType subjectGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public String subjectClientId() {
    return TestConstants.CLIENT_ID2;
  }

  @Value.Default
  public String subjectClientSecret() {
    return TestConstants.CLIENT_SECRET2;
  }

  @Value.Default
  public List<String> subjectScopes() {
    return List.of(TestConstants.SCOPE2);
  }

  @Value.Default
  public Map<String, String> subjectTokenConfig() {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(OAuth2Properties.Basic.GRANT_TYPE, subjectGrantType().commonName())
            .put(OAuth2Properties.Basic.CLIENT_ID, subjectClientId())
            .put(OAuth2Properties.Basic.CLIENT_SECRET, subjectClientSecret())
            .put(OAuth2Properties.Basic.EXTRA_PARAMS_PREFIX + "extra2", "value2");
    ConfigUtils.scopesAsString(subjectScopes())
        .ifPresent(scope -> builder.put(OAuth2Properties.Basic.SCOPE, scope));
    return builder.build();
  }

  @Value.Default
  @Nullable
  public String actorToken() {
    return TestConstants.ACTOR_TOKEN;
  }

  @Value.Default
  public URI actorTokenType() {
    return TestConstants.ACTOR_TOKEN_TYPE;
  }

  @Value.Default
  public GrantType actorGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public String actorClientId() {
    return TestConstants.CLIENT_ID1;
  }

  @Value.Default
  public String actorClientSecret() {
    return TestConstants.CLIENT_SECRET1;
  }

  @Value.Default
  public List<String> actorScopes() {
    return List.of(TestConstants.SCOPE1);
  }

  @Value.Default
  public Map<String, String> actorTokenConfig() {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(OAuth2Properties.Basic.GRANT_TYPE, actorGrantType().commonName())
            .put(OAuth2Properties.Basic.CLIENT_ID, actorClientId())
            .put(OAuth2Properties.Basic.CLIENT_SECRET, actorClientSecret())
            .put(OAuth2Properties.Basic.EXTRA_PARAMS_PREFIX + "extra2", "value2");
    ConfigUtils.scopesAsString(actorScopes())
        .ifPresent(scope -> builder.put(OAuth2Properties.Basic.SCOPE, scope));
    return builder.build();
  }

  @Value.Default
  public URI requestedTokenType() {
    return TestConstants.REQUESTED_TOKEN_TYPE;
  }

  @Value.Default
  @Nullable
  public String audience() {
    return TestConstants.AUDIENCE;
  }

  @Value.Default
  @Nullable
  public URI resource() {
    return TestConstants.RESOURCE;
  }

  @Value.Default
  public RuntimeConfig runtimeConfig() {
    RuntimeConfig.Builder builder =
        RuntimeConfig.builder().clock(clock()).agentName(agentName()).console(console());
    return builder.build();
  }

  @Value.Default
  public Clock clock() {
    return new TestClock(TestConstants.NOW);
  }

  @Value.Default
  public String agentName() {
    return "iceberg-auth-manager-" + FlowUtils.randomAlphaNumString(4);
  }

  @Value.Derived
  public PrintStream console() {
    return user().console();
  }

  @Value.Default
  public boolean forceInactiveUser() {
    return false;
  }

  @Value.Default
  public UserBehavior userBehavior() {
    return unitTest() ? UserBehavior.UNIT_TESTS : UserBehavior.INTEGRATION_TESTS;
  }

  @Value.Default
  public UserEmulator user() {
    if (forceInactiveUser()) {
      return UserEmulator.INACTIVE;
    } else {
      GrantType mainGrant = basicConfig().grantType();
      GrantType subjectGrant =
          mainGrant == GrantType.TOKEN_EXCHANGE
                  && tokenExchangeConfig().subjectToken().isEmpty()
                  && tokenExchangeConfig()
                      .subjectTokenConfig()
                      .containsKey(OAuth2Properties.Basic.GRANT_TYPE)
              ? GrantType.fromConfigName(
                  tokenExchangeConfig().subjectTokenConfig().get(OAuth2Properties.Basic.GRANT_TYPE))
              : GrantType.CLIENT_CREDENTIALS;
      GrantType actorGrant =
          mainGrant == GrantType.TOKEN_EXCHANGE
                  && tokenExchangeConfig().actorToken().isEmpty()
                  && tokenExchangeConfig()
                      .actorTokenConfig()
                      .containsKey(OAuth2Properties.Basic.GRANT_TYPE)
              ? GrantType.fromConfigName(
                  tokenExchangeConfig().actorTokenConfig().get(OAuth2Properties.Basic.GRANT_TYPE))
              : GrantType.CLIENT_CREDENTIALS;
      // If any of the grants require user interaction, use an interactive user emulator
      // Otherwise, use an inactive user emulator.
      if (mainGrant.requiresUserInteraction()
          || subjectGrant.requiresUserInteraction()
          || actorGrant.requiresUserInteraction()) {
        return new InteractiveUserEmulator(userBehavior());
      }
    }

    return UserEmulator.INACTIVE;
  }

  @Value.Default
  public Map<String, String> catalogProperties() {
    return ImmutableMap.<String, String>builder()
        .put(CatalogProperties.URI, catalogServerUrl().toString())
        .put("prefix", TestConstants.WAREHOUSE)
        .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
        .put(AuthProperties.AUTH_TYPE, OAuth2Manager.class.getName())
        .put(OAuth2Properties.Basic.GRANT_TYPE, grantType().toString())
        .put(OAuth2Properties.Basic.ISSUER_URL, authorizationServerUrl().toString())
        .put(OAuth2Properties.Basic.CLIENT_ID, clientId())
        .put(OAuth2Properties.Basic.CLIENT_SECRET, clientSecret())
        .put(
            OAuth2Properties.Basic.SCOPE,
            ConfigUtils.scopesAsString(scopes()).orElse(TestConstants.SCOPE1))
        .put(OAuth2Properties.Basic.EXTRA_PARAMS_PREFIX + "extra1", "value1")
        .put(OAuth2Properties.Runtime.AGENT_NAME, agentName())
        .build();
  }

  @Value.Default
  public SessionCatalog.SessionContext sessionContext() {
    return SessionCatalog.SessionContext.createEmpty();
  }

  @Value.Default
  public Map<String, String> tableProperties() {
    return Map.of();
  }

  public RESTCatalog createCatalog(boolean initialize) {
    RESTCatalog catalog =
        new RESTCatalog(sessionContext(), config -> newHttpClientBuilder(config).build());
    UserEmulator user = user();
    user.addErrorListener(
        e -> {
          try {
            catalog.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        });
    if (initialize) {
      catalog.initialize("catalog-" + FlowUtils.randomAlphaNumString(4), catalogProperties());
    }

    return catalog;
  }

  public FlowFactory createFlowFactory() {
    return FlowFactory.of(agentSpec(), executor(), this::httpClient);
  }

  public OAuth2Agent createAgent() {
    OAuth2Agent agent = new OAuth2Agent(agentSpec(), executor(), this::httpClient);
    user().addErrorListener(e -> agent.close());
    return agent;
  }

  public void createExpectations() {
    ImmutableClientCredentialsExpectation.of(this).create();
    ImmutablePasswordExpectation.of(this).create();
    ImmutableAuthorizationCodeExpectation.of(this).create();
    ImmutableDeviceCodeExpectation.of(this).create();
    ImmutableTokenExchangeExpectation.of(this).create();
    ImmutableRefreshTokenExpectation.of(this).create();
    createMetadataDiscoveryExpectations();
    createCatalogExpectations();
    createErrorExpectations();
  }

  public void createMetadataDiscoveryExpectations() {
    ImmutableMetadataDiscoveryExpectation.of(this).create();
  }

  public void createCatalogExpectations() {
    ImmutableConfigEndpointExpectation.of(this).create();
    ImmutableLoadTableEndpointExpectation.of(this).create();
  }

  public void createErrorExpectations() {
    ImmutableErrorExpectation.of(this).create();
  }
}
