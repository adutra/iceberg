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
package org.apache.iceberg.rest.oauth2.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.IcebergCoreHooks;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.oauth2.auth.ClientAuthentication;
import org.apache.iceberg.rest.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.oauth2.endpoint.EndpointProvider;
import org.apache.iceberg.rest.oauth2.endpoint.EndpointProviderFactory;
import org.apache.iceberg.rest.oauth2.flow.FlowFactory;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.test.expectation.ImmutableClientCredentialsExpectation;
import org.apache.iceberg.rest.oauth2.test.expectation.ImmutableErrorExpectation;
import org.apache.iceberg.rest.oauth2.test.expectation.ImmutableMetadataDiscoveryExpectation;
import org.apache.iceberg.rest.oauth2.test.server.HttpServer;
import org.apache.iceberg.rest.oauth2.test.server.MockHttpServer;
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
  public boolean discoveryEnabled() {
    return true;
  }

  @Value.Default
  public boolean createDefaultExpectations() {
    return true;
  }

  @Value.Lazy
  public HttpServer server() {
    return new MockHttpServer();
  }

  @Value.Default
  public HTTPClient httpClient() {
    return HTTPClient.builder(Map.of()).withAuthSession(AuthSession.EMPTY).build();
  }

  @Value.Default
  public ScheduledExecutorService executor() {
    return ThreadPools.newScheduledPool("token-refresh", executorPoolSize());
  }

  @Value.Default
  public int executorPoolSize() {
    return 1;
  }

  @Value.Lazy
  public EndpointProvider endpointProvider() {
    return EndpointProviderFactory.createEndpointProvider(basicConfig(), this::httpClient);
  }

  @Override
  public void close() {
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
    return server().rootUrl();
  }

  @Value.Default
  public String authorizationServerContextPath() {
    return "/realms/master/";
  }

  @Value.Default
  public URI authorizationServerUrl() {
    return serverRootUrl().resolve(authorizationServerContextPath());
  }

  @Value.Default
  public URI tokenEndpoint() {
    return authorizationServerUrl().resolve("protocol/openid-connect/token");
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
  public BasicConfig basicConfig() {
    BasicConfig.Builder builder =
        BasicConfig.builder()
            .scopes(scopes())
            .extraRequestParameters(Map.of("extra1", "value1"))
            .grantType(grantType())
            .minTimeout(timeout())
            .timeout(timeout())
            .clientId(clientId())
            .clientSecret(clientSecret());

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
  public Clock clock() {
    return new TestClock(TestConstants.NOW);
  }

  public FlowFactory createFlowFactory() {
    return FlowFactory.of(basicConfig(), clock(), executor(), this::httpClient);
  }

  public void createExpectations() {
    ImmutableClientCredentialsExpectation.of(this).create();
    createMetadataDiscoveryExpectations();
    createErrorExpectations();
  }

  public void createMetadataDiscoveryExpectations() {
    ImmutableMetadataDiscoveryExpectation.of(this).create();
  }

  public void createErrorExpectations() {
    ImmutableErrorExpectation.of(this).create();
  }
}
