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
package org.apache.iceberg.rest.auth.oauth2.rest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.immutables.value.Value.Check;

/**
 * Common base for all requests to the token endpoint.
 *
 * @see ClientCredentialsTokenRequest
 * @see AuthorizationCodeTokenRequest
 * @see RefreshTokenRequest
 * @see TokenExchangeRequest
 */
public interface TokenRequest extends ClientRequest, RESTRequest {

  String GRANT_TYPE = "grant_type";
  String SCOPE = "scope";

  /** The authorization grant type. */
  GrantType grantType();

  /**
   * OPTIONAL, if identical to the scope requested by the client; otherwise, REQUIRED. The scope of
   * the access token as described by <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">Section 3.3</a>.
   *
   * <p>In case of refresh, the requested scope MUST NOT include any scope not originally granted by
   * the resource owner, and if omitted is treated as equal to the scope originally granted by the
   * resource owner.
   */
  @Nullable
  String scope();

  /**
   * Additional parameters to be included in the request. This is useful for custom parameters that
   * are not covered by the standard OAuth2.0 specification.
   */
  Map<String, String> extraParameters();

  @Override
  default Map<String, String> asFormParameters() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder
        .putAll(extraParameters())
        .putAll(ClientRequest.super.asFormParameters())
        .put(GRANT_TYPE, grantType().canonicalName());

    String scope = scope();
    if (scope != null) {
      builder.put(SCOPE, scope);
    }

    return builder.buildKeepingLast();
  }

  @Override
  @Check
  default void validate() {
    // Already validated by Immutables
  }

  interface Builder<T extends TokenRequest, B extends Builder<T, B>>
      extends ClientRequest.Builder<T, B> {

    @CanIgnoreReturnValue
    B scope(String scope);

    @CanIgnoreReturnValue
    B extraParameters(Map<String, ? extends String> extraParameters);
  }
}
