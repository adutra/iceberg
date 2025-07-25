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
package org.apache.iceberg.rest.oauth2.flow;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.oauth2.agent.OAuth2AgentSpec;
import org.apache.iceberg.rest.oauth2.auth.ClientAuthenticator;
import org.apache.iceberg.rest.oauth2.auth.ClientAuthenticatorFactory;
import org.apache.iceberg.rest.oauth2.endpoint.EndpointProvider;
import org.apache.iceberg.rest.oauth2.endpoint.EndpointProviderFactory;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.tokenexchange.ActorTokenSupplier;
import org.apache.iceberg.rest.oauth2.tokenexchange.SubjectTokenSupplier;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public abstract class FlowFactory implements AutoCloseable {

  public static FlowFactory of(
      OAuth2AgentSpec spec,
      ScheduledExecutorService executor,
      Supplier<RESTClient> restClientSupplier) {
    return ImmutableFlowFactory.builder()
        .spec(spec)
        .executor(executor)
        .restClientSupplier(restClientSupplier)
        .build();
  }

  /** Creates a flow for fetching new tokens. This is used for the initial token fetch. */
  public InitialFlow createInitialFlow() {
    return newInitialFlowBuilder()
        .spec(getSpec())
        .executor(getExecutor())
        .restClient(getRestClientSupplier().get())
        .endpointProvider(getEndpointProvider())
        .clientAuthenticator(getClientAuthenticator())
        .build();
  }

  /**
   * Creates a flow for refreshing tokens. This is used for refreshing tokens when the access token
   * expires.
   */
  public RefreshFlow createTokenRefreshFlow() {
    return newTokenRefreshFlowBuilder()
        .spec(getSpec())
        .executor(getExecutor())
        .restClient(getRestClientSupplier().get())
        .endpointProvider(getEndpointProvider())
        .clientAuthenticator(getClientAuthenticator())
        .build();
  }

  @Override
  @SuppressWarnings({"EmptyBlock", "EmptyTryBlock"})
  public void close() {
    if (getSpec().basicConfig().grantType() == GrantType.TOKEN_EXCHANGE) {
      SubjectTokenSupplier subjectTokenSupplier = getSubjectTokenSupplier();
      ActorTokenSupplier actorTokenSupplier = getActorTokenSupplier();
      try (subjectTokenSupplier;
          actorTokenSupplier) {}
    }
  }

  public FlowFactory copy() {
    SubjectTokenSupplier subjectTokenSupplier = getSubjectTokenSupplier();
    ActorTokenSupplier actorTokenSupplier = getActorTokenSupplier();
    return ImmutableFlowFactory.builder()
        .from(this)
        // Copy the token suppliers to also create copies of their internal agents.
        .subjectTokenSupplier(subjectTokenSupplier == null ? null : subjectTokenSupplier.copy())
        .actorTokenSupplier(actorTokenSupplier == null ? null : actorTokenSupplier.copy())
        .build();
  }

  protected abstract OAuth2AgentSpec getSpec();

  protected abstract ScheduledExecutorService getExecutor();

  protected abstract Supplier<RESTClient> getRestClientSupplier();

  @Value.Default
  protected EndpointProvider getEndpointProvider() {
    return EndpointProviderFactory.createEndpointProvider(getSpec(), getRestClientSupplier());
  }

  @Value.Default
  protected ClientAuthenticator getClientAuthenticator() {
    return ClientAuthenticatorFactory.createAuthenticator(
        getSpec(), getEndpointProvider().resolvedTokenEndpoint());
  }

  @Value.Default
  @Nullable
  protected SubjectTokenSupplier getSubjectTokenSupplier() {
    return getSpec().basicConfig().grantType() != GrantType.TOKEN_EXCHANGE
        ? null
        : SubjectTokenSupplier.of(getSpec(), getExecutor(), getRestClientSupplier());
  }

  @Value.Default
  @Nullable
  protected ActorTokenSupplier getActorTokenSupplier() {
    return getSpec().basicConfig().grantType() != GrantType.TOKEN_EXCHANGE
        ? null
        : ActorTokenSupplier.of(getSpec(), getExecutor(), getRestClientSupplier());
  }

  private AbstractFlow.Builder<? extends InitialFlow, ?> newInitialFlowBuilder() {
    switch (getSpec().basicConfig().grantType()) {
      case CLIENT_CREDENTIALS:
        return ImmutableClientCredentialsFlow.builder();
      case PASSWORD:
        return ImmutableResourceOwnerPasswordFlow.builder();
      case AUTHORIZATION_CODE:
        return ImmutableAuthorizationCodeFlow.builder();
      case DEVICE_CODE:
        return ImmutableDeviceCodeFlow.builder();
      case TOKEN_EXCHANGE:
        SubjectTokenSupplier subjectTokenSupplier =
            Preconditions.checkNotNull(
                getSubjectTokenSupplier(), "Invalid subject token supplier: null");
        ActorTokenSupplier actorTokenSupplier =
            Preconditions.checkNotNull(
                getActorTokenSupplier(), "Invalid actor token supplier: null");
        return ImmutableTokenExchangeFlow.builder()
            .subjectTokenStage(subjectTokenSupplier.supplyTokenAsync())
            .actorTokenStage(actorTokenSupplier.supplyTokenAsync());
      default:
        throw new IllegalArgumentException(
            "Unknown or invalid grant type for initial token fetch: "
                + getSpec().basicConfig().grantType());
    }
  }

  private AbstractFlow.Builder<? extends RefreshFlow, ?> newTokenRefreshFlowBuilder() {
    switch (getSpec().basicConfig().dialect()) {
      case STANDARD:
        return ImmutableRefreshTokenFlow.builder();
      case ICEBERG_REST:
        return ImmutableIcebergRefreshTokenFlow.builder();
      default:
        throw new IllegalArgumentException(
            "Unknown or invalid dialect: " + getSpec().basicConfig().dialect());
    }
  }
}
