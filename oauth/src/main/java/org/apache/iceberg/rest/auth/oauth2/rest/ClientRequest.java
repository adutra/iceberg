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
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;
import org.immutables.value.Value.Redacted;

/**
 * Common interface for requests using where the client may authenticate with request body
 * parameters.
 *
 * @see ClientCredentialsTokenRequest
 * @see PasswordTokenRequest
 * @see AuthorizationCodeTokenRequest
 * @see DeviceAccessTokenRequest
 * @see DeviceAuthorizationRequest
 * @see PasswordTokenRequest
 * @see RefreshTokenRequest
 * @see TokenExchangeRequest
 */
public interface ClientRequest extends PostFormRequest {

  String CLIENT_ID = "client_id";
  String CLIENT_SECRET = "client_secret";
  String CLIENT_ASSERTION = "client_assertion";
  String CLIENT_ASSERTION_TYPE = "client_assertion_type";

  /**
   * The client identifier as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.2">RFC 6749 Section 2.2</a>.
   */
  @Nullable
  String clientId();

  /**
   * The client password as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1">RFC 6749 Section 2.3.1</a>.
   */
  @Nullable
  @Redacted
  @SuppressWarnings("SafeLoggingPropagation")
  String clientSecret();

  /**
   * The client assertion as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc7523#section-2.2">RFC 7523 Section 2.2.</a>.
   *
   * <p>This is typically a JWT (JSON Web Token) used to assert the identity of the client to the
   * authorization server. Only used when the client is using a client assertion for authentication
   * instead of a client secret.
   */
  @Nullable
  TypedToken clientAssertion();

  @Override
  default Map<String, String> asFormParameters() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    String clientId = clientId();
    if (clientId != null) {
      builder.put(CLIENT_ID, clientId);
    }

    String clientSecret = clientSecret();
    if (clientSecret != null) {
      builder.put(CLIENT_SECRET, clientSecret);
    }

    TypedToken clientAssertion = clientAssertion();
    if (clientAssertion != null) {
      builder.put(CLIENT_ASSERTION, clientAssertion.payload());
      builder.put(CLIENT_ASSERTION_TYPE, clientAssertion.tokenType().toString());
    }

    return builder.build();
  }

  interface Builder<T extends ClientRequest, B extends Builder<T, B>> {

    @CanIgnoreReturnValue
    B clientId(String clientId);

    @CanIgnoreReturnValue
    B clientSecret(String clientSecret);

    @CanIgnoreReturnValue
    B clientAssertion(TypedToken clientAssertion);

    T build();
  }
}
