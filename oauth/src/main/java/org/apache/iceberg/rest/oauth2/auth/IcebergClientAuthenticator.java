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
package org.apache.iceberg.rest.oauth2.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.config.Secret;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.oauth2.rest.ClientRequest;
import org.apache.iceberg.rest.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.rest.oauth2.token.AccessToken;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.immutables.value.Value;

/** A non-standard client authenticator targeting the Iceberg REST dialect. */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class IcebergClientAuthenticator implements ClientAuthenticator {

  abstract Optional<String> clientId();

  abstract Optional<Secret> clientSecret();

  @Override
  public final <R extends ClientRequest, B extends ClientRequest.Builder<R, B>> void authenticate(
      ClientRequest.Builder<R, B> request,
      Map<String, String> headers,
      @Nullable Tokens currentTokens) {
    if (request instanceof ClientCredentialsTokenRequest.Builder) {
      // initial token fetches: use client_secret_post style, except that
      // both client id and client secret could be absent
      clientId().ifPresent(request::clientId);
      clientSecret().map(Secret::value).ifPresent(request::clientSecret);
    } else if (request instanceof TokenExchangeRequest.Builder) {
      // token refreshes: use client_secret_basic style if possible,
      // otherwise bearer token (non-standard)
      if (clientId().isPresent() && clientSecret().isPresent()) {
        String credentials = clientId().get() + ":" + clientSecret().get().value();
        String auth =
            Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        headers.put("Authorization", "Basic " + auth);
      } else {
        AccessToken accessToken =
            Preconditions.checkNotNull(currentTokens, "Invalid currentTokens: null").accessToken();
        headers.put("Authorization", "Bearer " + accessToken.payload());
      }
    } else {
      throw new IllegalArgumentException(
          "Unsupported request builder type for Iceberg REST dialect: "
              + request.getClass().getName());
    }
  }
}
