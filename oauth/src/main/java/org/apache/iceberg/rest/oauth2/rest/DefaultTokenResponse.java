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
package org.apache.iceberg.rest.oauth2.rest;

import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * The default response in reply to a {@link TokenRequest}.
 *
 * <p>Most OAuth 2.0 flows return exactly the same response. These responses share the same schema,
 * which is declared in <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-5.1">Section
 * 5.1</a>.
 *
 * <p>Example of response:
 *
 * <pre>{@code
 * HTTP/1.1 200 OK
 * Content-Type: application/json;charset=UTF-8
 * Cache-Control: no-store
 * Pragma: no-cache
 *
 * {
 *   "access_token":"2YotnFZFEjr1zCsicMWpAA",
 *   "token_type":"example",
 *   "expires_in":3600,
 *   "example_parameter":"example_value"
 * }
 * }</pre>
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-5.1">Access Token
 *     Response</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc8628#section-3.5">Device Access Token
 *     Response</a>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public interface DefaultTokenResponse extends TokenResponse {

  interface Builder extends TokenResponse.Builder<DefaultTokenResponse, Builder> {}
}
