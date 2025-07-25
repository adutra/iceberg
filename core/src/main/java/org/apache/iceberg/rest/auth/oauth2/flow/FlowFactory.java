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
package org.apache.iceberg.rest.auth.oauth2.flow;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2ClientRuntime;
import org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig;
import org.apache.iceberg.rest.auth.oauth2.endpoint.EndpointProvider;
import org.apache.iceberg.rest.auth.oauth2.http.HttpClient;
import org.apache.iceberg.rest.auth.oauth2.tokenexchange.ActorTokenSupplier;
import org.apache.iceberg.rest.auth.oauth2.tokenexchange.SubjectTokenSupplier;
import org.immutables.value.Value;

/**
 * A factory for creating {@link Flow} instances. This is one of the main components of the OAuth2
 * client, responsible for creating flows for fetching new tokens and refreshing tokens.
 */
@Value.Immutable
public abstract class FlowFactory implements AutoCloseable {

  public static FlowFactory create(OAuth2Config config, OAuth2ClientRuntime runtime) {
    return ImmutableFlowFactory.builder().config(config).runtime(runtime).build();
  }

  /** Creates a flow for fetching new tokens. This is used for the initial token fetch. */
  public Flow newInitialFlow() {
    return newInitialFlowBuilder()
        .config(config())
        .runtime(runtime())
        .endpointProvider(endpointProvider())
        .requestSender(httpClient())
        .build();
  }

  /**
   * Creates a flow for refreshing tokens. This is used for refreshing tokens when the access token
   * expires.
   */
  public Flow newRefreshFlow(Tokens currentTokens) {
    return newRefreshFlowBuilder(currentTokens)
        .config(config())
        .runtime(runtime())
        .endpointProvider(endpointProvider())
        .requestSender(httpClient())
        .build();
  }

  @Override
  @SuppressWarnings({"EmptyTryBlock", "EmptyBlock"})
  public void close() {
    SubjectTokenSupplier subjectTokenSupplier = subjectTokenSupplier();
    ActorTokenSupplier actorTokenSupplier = actorTokenSupplier();
    HttpClient httpClient = httpClient();
    try (httpClient;
        subjectTokenSupplier;
        actorTokenSupplier) {}
  }

  public FlowFactory copy() {
    @SuppressWarnings("resource")
    SubjectTokenSupplier subjectTokenSupplier = subjectTokenSupplier();
    @SuppressWarnings("resource")
    ActorTokenSupplier actorTokenSupplier = actorTokenSupplier();
    return ImmutableFlowFactory.builder()
        .from(this)
        // Copy the token suppliers to also create copies of their internal clients.
        .subjectTokenSupplier(subjectTokenSupplier == null ? null : subjectTokenSupplier.copy())
        .actorTokenSupplier(actorTokenSupplier == null ? null : actorTokenSupplier.copy())
        .build();
  }

  protected abstract OAuth2Config config();

  protected abstract OAuth2ClientRuntime runtime();

  @Value.Lazy
  @SuppressWarnings("MustBeClosedChecker")
  protected HttpClient httpClient() {
    HttpClientConfig httpClientConfig = config().httpClientConfig();
    return httpClientConfig.clientType().newHttpClient(httpClientConfig);
  }

  @Value.Default
  protected EndpointProvider endpointProvider() {
    return EndpointProvider.create(config(), httpClient());
  }

  @Value.Default
  @Nullable
  protected SubjectTokenSupplier subjectTokenSupplier() {
    return !config().basicConfig().grantType().equals(GrantType.TOKEN_EXCHANGE)
        ? null
        : SubjectTokenSupplier.create(config(), runtime());
  }

  @Value.Default
  @Nullable
  protected ActorTokenSupplier actorTokenSupplier() {
    return !config().basicConfig().grantType().equals(GrantType.TOKEN_EXCHANGE)
        ? null
        : ActorTokenSupplier.create(config(), runtime());
  }

  private FlowBase.Builder<? extends Flow, ?> newInitialFlowBuilder() {

    GrantType grantType = config().basicConfig().grantType();

    if (grantType.equals(GrantType.CLIENT_CREDENTIALS)) {
      return ImmutableClientCredentialsFlow.builder();

    } else if (grantType.equals(GrantType.PASSWORD)) {
      return ImmutablePasswordFlow.builder();

    } else if (grantType.equals(GrantType.AUTHORIZATION_CODE)) {
      return ImmutableAuthorizationCodeFlow.builder();

    } else if (grantType.equals(GrantType.DEVICE_CODE)) {
      return ImmutableDeviceCodeFlow.builder();

    } else if (grantType.equals(GrantType.TOKEN_EXCHANGE)) {
      SubjectTokenSupplier subjectTokenSupplier =
          Preconditions.checkNotNull(
              subjectTokenSupplier(), "Invalid subject token supplier: null");
      ActorTokenSupplier actorTokenSupplier =
          Preconditions.checkNotNull(actorTokenSupplier(), "Invalid actor token supplier: null");
      return ImmutableTokenExchangeFlow.builder()
          .subjectTokenStage(subjectTokenSupplier.supplyTokenAsync())
          .actorTokenStage(actorTokenSupplier.supplyTokenAsync());
    }

    throw new IllegalArgumentException(
        "Unknown or invalid grant type for initial token fetch: "
            + config().basicConfig().grantType());
  }

  private FlowBase.Builder<? extends Flow, ?> newRefreshFlowBuilder(Tokens currentTokens) {

    GrantType grantType = config().tokenRefreshConfig().grantType();

    if (grantType.equals(GrantType.REFRESH_TOKEN)) {
      return ImmutableRefreshTokenFlow.builder().refreshToken(currentTokens.getRefreshToken());

    } else if (grantType.equals(GrantType.TOKEN_EXCHANGE)) {
      return ImmutableTokenExchangeFlow.builder()
          .subjectTokenStage(CompletableFuture.completedFuture(currentTokens.getAccessToken()))
          .actorTokenStage(CompletableFuture.completedFuture(null));
    }

    throw new IllegalArgumentException(
        "Unknown or invalid grant type for token refresh: " + config().basicConfig().grantType());
  }
}
