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

import java.util.Map;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.rest.ClientRequest;
import org.immutables.value.Value;

/**
 * A Client authentication method for clients in possession of a client password, using request body
 * parameters.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1">OAuth 2.0
 *     specification, Section 2.3.1</a>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class ClientSecretPostAuthenticator implements ClientSecretAuthenticator {

  @Override
  public final <R extends ClientRequest, B extends ClientRequest.Builder<R, B>> void authenticate(
      ClientRequest.Builder<R, B> request, Map<String, String> headers) {
    request.clientId(clientId()).clientSecret(clientSecret().value());
  }
}
