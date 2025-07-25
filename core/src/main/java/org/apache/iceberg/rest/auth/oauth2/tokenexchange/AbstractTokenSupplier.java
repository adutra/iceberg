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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2ClientRuntime;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableBasicConfig;
import org.immutables.value.Value;

public abstract class AbstractTokenSupplier implements AutoCloseable {

  /**
   * Returns a stage that will supply the requested token when completed.
   *
   * <p>If the token is static, the returned stage will be already completed with the token to use.
   * Otherwise, the stage will complete when the underlying client has completed its authentication.
   *
   * <p>If no token is configured, the returned stage will be already completed, with a null value.
   */
  public CompletionStage<AccessToken> supplyTokenAsync() {
    @SuppressWarnings("resource")
    OAuth2Client tokenClient = tokenClient();
    if (tokenClient != null) {
      return tokenClient.authenticateAsync();
    }

    if (staticToken().isPresent()) {
      TypelessAccessToken token = staticToken().get();
      BearerAccessToken accessToken =
          new BearerAccessToken(token.getValue(), 0, null, staticTokenType());
      return CompletableFuture.completedFuture(accessToken);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Returns a copy of this token supplier. The copy will share the same spec, executor and REST
   * client supplier as the original supplier, as well as its static token, if any. If the token is
   * dynamic, the original client will be copied.
   */
  public abstract AbstractTokenSupplier copy();

  /**
   * Returns the client to use for fetching the token. Returns null if the token is static or not
   * configured.
   */
  @Value.Default
  @Nullable
  protected OAuth2Client tokenClient() {
    if (!mainConfig().basicConfig().grantType().equals(GrantType.TOKEN_EXCHANGE)
        || staticToken().isPresent()
        || dynamicTokenConfig().isEmpty()) {
      return null;
    }

    OAuth2Config clientConfig = dynamicTokenConfig().get();
    if (clientConfig.basicConfig().clientName().isEmpty()) {
      clientConfig =
          ImmutableOAuth2Config.builder()
              .from(clientConfig)
              .basicConfig(
                  ImmutableBasicConfig.builder()
                      .from(clientConfig.basicConfig())
                      .clientName(defaultClientName())
                      .build())
              .build();
    }

    return new OAuth2Client(clientConfig, runtime());
  }

  @Override
  public void close() {
    OAuth2Client client = tokenClient();
    if (client != null) {
      client.close();
    }
  }

  protected abstract OAuth2Config mainConfig();

  protected abstract OAuth2ClientRuntime runtime();

  @Value.Derived
  protected abstract Optional<TypelessAccessToken> staticToken();

  @Value.Derived
  protected abstract TokenTypeURI staticTokenType();

  @Value.Derived
  protected abstract Optional<OAuth2Config> dynamicTokenConfig();

  @Value.Derived
  protected abstract String defaultClientName();
}
