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
import org.immutables.value.Value.Redacted;

/**
 * Common interface for requests using where the client may authenticate with request body
 * parameters.
 *
 * @see ClientCredentialsTokenRequest
 */
public interface ClientRequest extends PostFormRequest {

  String CLIENT_ID = "client_id";
  String CLIENT_SECRET = "client_secret";

  /**
   * The client identifier as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.2">Section 2.2</a>.
   */
  @Nullable
  String clientId();

  /**
   * The client password as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1">Section 2.3.1</a>.
   */
  @Nullable
  @Redacted
  @SuppressWarnings("SafeLoggingPropagation")
  String clientSecret();

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

    return builder.build();
  }

  interface Builder<T extends ClientRequest, B extends Builder<T, B>> {

    @CanIgnoreReturnValue
    B clientId(String clientId);

    @CanIgnoreReturnValue
    B clientSecret(String clientSecret);

    T build();
  }
}
