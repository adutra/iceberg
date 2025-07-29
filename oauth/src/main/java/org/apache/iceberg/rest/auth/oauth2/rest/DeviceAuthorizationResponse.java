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
import javax.annotation.Nullable;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * A device authorization response as defined in <a
 * href="https://tools.ietf.org/html/rfc8628#section-3.2">RFC 8628 Section 3.2</a>.
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
 * "device_code":"GmRhmhcxhwAzkoEqiMEg_DnyEysNkuNhszIySk9eS",
 * "user_code":"WDJB-MJHT",
 * "verification_uri":"https://example.com/device",
 * "verification_uri_complete":"https://example.com/device?user_code=WDJB-MJHT",
 * "expires_in":1800,
 * "interval":5
 * }
 * }</pre>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class DeviceAuthorizationResponse implements RESTResponse {

  @Override
  public final void validate() {}

  /** The device verification code. */
  public abstract String deviceCode();

  /** The end-user verification code. */
  public abstract String userCode();

  /**
   * The end-user verification URI on the authorization server. The URI should be short and easy to
   * remember as end users will be asked to manually type it into their user agent.
   */
  public abstract URI verificationUri();

  /**
   * OPTIONAL. A verification URI that includes the "user_code" (or other information with the same
   * function as the "user_code"), which is designed for non-textual transmission.
   */
  @Nullable
  public abstract URI verificationUriComplete();

  /** The lifetime in seconds of the "device_code" and "user_code". */
  public abstract int expiresInSeconds();

  /**
   * OPTIONAL. The minimum amount of time in seconds that the client SHOULD wait between polling
   * requests to the token endpoint. If no value is provided, clients MUST use 5 as the default.
   */
  @Nullable
  public abstract Integer intervalSeconds();
}
