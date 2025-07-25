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

import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.util.Optional;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2ClientRuntime;
import org.immutables.value.Value;

/** A component that centralizes the logic for supplying the actor token for token exchanges. */
@Value.Immutable
public abstract class ActorTokenSupplier extends AbstractTokenSupplier {

  public static ActorTokenSupplier create(OAuth2Config config, OAuth2ClientRuntime runtime) {
    return ImmutableActorTokenSupplier.builder().mainConfig(config).runtime(runtime).build();
  }

  @Override
  public ActorTokenSupplier copy() {
    @SuppressWarnings("resource")
    OAuth2Client tokenClient = tokenClient();
    return ImmutableActorTokenSupplier.builder()
        .from(this)
        .tokenClient(tokenClient == null ? null : tokenClient.copy())
        .build();
  }

  @Override
  protected Optional<TypelessAccessToken> staticToken() {
    return mainConfig().tokenExchangeConfig().actorToken();
  }

  @Override
  protected TokenTypeURI staticTokenType() {
    return mainConfig().tokenExchangeConfig().actorTokenType();
  }

  @Override
  protected Optional<OAuth2Config> dynamicTokenConfig() {
    return mainConfig().tokenExchangeConfig().actorTokenConfig();
  }

  @Override
  protected String defaultClientName() {
    return mainConfig().basicConfig().clientName().orElse(OAuth2Client.DEFAULT_CLIENT_NAME)
        + "-actor";
  }
}
