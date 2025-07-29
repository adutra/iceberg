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
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * Successful response in reply to a {@link TokenRequest} for the Token Exchange grant type.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc8693/#section-2.2.1">Token Exchange
 *     Response</a>
 */
@Value.Immutable
@OAuth2ImmutableStyle
@SuppressWarnings("immutables:subtype")
public interface TokenExchangeResponse extends TokenResponse {

  /**
   * REQUIRED. An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, for the
   * representation of the issued security token.
   *
   * <p>This field is generally set to a known token type, see {@link
   * org.apache.iceberg.rest.auth.oauth2.token.TypedToken} for the list of standard token types.
   *
   * <p>Note: this field is required by the spec, but some token endpoints do not return it,
   * especially those using Iceberg REST dialect. This is why we allow it to be nullable.
   *
   * <p>When using Keycloak, this field is set to the value of the {@code requested_token_type}
   * parameter in the request. See <a
   * href="https://www.keycloak.org/securing-apps/token-exchange#_standard-token-exchange-request">Request
   * and response parameters</a>.
   *
   * <p>The OAuth2 agent does not validate nor inspect the value of this field. It is included here
   * for completeness.
   */
  @Nullable
  URI issuedTokenType();

  interface Builder
      extends TokenResponse.Builder<TokenExchangeResponse, TokenExchangeResponse.Builder> {

    @CanIgnoreReturnValue
    Builder issuedTokenType(URI issuedTokenType);
  }
}
