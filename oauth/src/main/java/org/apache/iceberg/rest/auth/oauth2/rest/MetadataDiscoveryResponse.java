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

import java.net.URI;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * The response from the OpenID Connect Discovery endpoint.
 *
 * @see <a href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata">OpenID
 *     Connect Discovery 1.0</a>
 * @see <a href="https://tools.ietf.org/html/rfc8414#section-5">RFC 8414 Section 5</a>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class MetadataDiscoveryResponse implements RESTResponse {

  @Override
  public final void validate() {}

  /**
   * URL using the https scheme with no query or fragment components that the OP asserts as its
   * Issuer Identifier.
   */
  public abstract URI issuerUrl();

  /**
   * URL of the OP's OAuth 2.0 Token Endpoint. This is REQUIRED unless only the Implicit Flow is
   * used. This URL MUST use the https scheme and MAY contain port, path, and query parameter
   * components.
   */
  public abstract URI tokenEndpoint();

  /**
   * URL of the OP's OAuth 2.0 Authorization Endpoint. This URL MUST use the https scheme and MAY
   * contain port, path, and query parameter components.
   */
  public abstract URI authorizationEndpoint();
}
