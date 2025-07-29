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
package org.apache.iceberg.rest.auth.oauth2.tokenexchange;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.agent.OAuth2Agent;
import org.apache.iceberg.rest.auth.oauth2.agent.OAuth2AgentSpec;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;
import org.immutables.value.Value;

public abstract class AbstractTokenSupplier implements AutoCloseable {

  /**
   * Returns a stage that will supply the requested token when completed.
   *
   * <p>If the token is static, the returned stage will be already completed with the token to use.
   * Otherwise, the stage will complete when the underlying agent has completed its authentication.
   *
   * <p>If no token is configured, the returned stage will be already completed, with a null value.
   */
  public CompletionStage<TypedToken> supplyTokenAsync() {
    @SuppressWarnings("resource")
    OAuth2Agent agent = tokenAgent();
    if (agent != null) {
      return agent.authenticateAsync().thenApply(TypedToken::of);
    }

    return token().isPresent()
        ? CompletableFuture.completedFuture(TypedToken.of(token().get(), tokenType()))
        : CompletableFuture.completedFuture(null);
  }

  /**
   * Returns a copy of this token supplier. The copy will share the same spec, executor and REST
   * client supplier as the original supplier, as well as its static token, if any. If the token is
   * dynamic, the original agent will be copied.
   */
  public abstract AbstractTokenSupplier copy();

  /**
   * Returns the agent to use for fetching the token. Returns null if the token is static or not
   * configured.
   */
  @Value.Default
  @Nullable
  protected OAuth2Agent tokenAgent() {
    if (mainSpec().basicConfig().grantType() != GrantType.TOKEN_EXCHANGE
        || token().isPresent()
        || agentConfig().isEmpty()) {
      return null;
    }

    Map<String, String> config = agentConfig();
    if (!config.containsKey(OAuth2Properties.Runtime.AGENT_NAME)) {
      config = Maps.newHashMap(config);
      config.put(OAuth2Properties.Runtime.AGENT_NAME, defaultAgentName());
    }

    OAuth2AgentSpec tokenSpec = mainSpec().merge(config);
    return new OAuth2Agent(tokenSpec, executor(), restClientSupplier());
  }

  @Override
  public void close() {
    OAuth2Agent agent = tokenAgent();
    if (agent != null) {
      agent.close();
    }
  }

  /**
   * Returns the main spec, which contains the configuration for the token exchange. This is used to
   * merge the token-specific configuration with the main configuration.
   */
  protected abstract OAuth2AgentSpec mainSpec();

  /** Returns the executor to use for instantiating the token agent if needed. */
  protected abstract ScheduledExecutorService executor();

  /** Returns the REST client supplier to use for instantiating the token agent if needed. */
  protected abstract Supplier<RESTClient> restClientSupplier();

  /**
   * Returns a static token to use, if any. If this token is not present, a token will be
   * dynamically fetched using the token agent.
   */
  protected abstract Optional<String> token();

  /** Returns the type of the static token returned by {@link #token()}. */
  protected abstract URI tokenType();

  /**
   * Returns the agent configuration to use for fetching a token dynamically. The properties
   * returned by this method are merged with the {@linkplain #mainSpec() main agent spec} in order
   * to create the agent configuration. Therefore, the returned map does not need to contain a
   * complete agent configuration; it should only contain additional properties, or property
   * overrides.
   */
  @Value.Derived
  protected abstract Map<String, String> agentConfig();

  /**
   * Returns the default agent name to use if the agent name is not specified in the {@linkplain
   * #agentConfig() agent configuration}. This should be distinct from the main agent name,
   * otherwise the agent messages on the console will be confusing.
   */
  protected abstract String defaultAgentName();
}
