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
package org.apache.iceberg.rest.oauth2.tokenexchange;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.oauth2.agent.OAuth2AgentSpec;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/** A component that centralizes the logic for supplying the actor token for token exchanges. */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class ActorTokenSupplier extends AbstractTokenSupplier {

  public static ActorTokenSupplier of(
      OAuth2AgentSpec spec,
      ScheduledExecutorService executor,
      Supplier<RESTClient> restClientSupplier) {
    return ImmutableActorTokenSupplier.builder()
        .mainSpec(spec)
        .executor(executor)
        .restClientSupplier(restClientSupplier)
        .build();
  }

  @Override
  protected Optional<String> token() {
    return mainSpec().tokenExchangeConfig().actorToken();
  }

  @Override
  protected URI tokenType() {
    return mainSpec().tokenExchangeConfig().actorTokenType();
  }

  @Override
  protected Map<String, String> agentConfig() {
    return mainSpec().tokenExchangeConfig().actorTokenConfig();
  }

  @Override
  protected String defaultAgentName() {
    return mainSpec().runtimeConfig().agentName() + "-actor";
  }
}
