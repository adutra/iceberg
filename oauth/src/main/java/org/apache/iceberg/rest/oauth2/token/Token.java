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
package org.apache.iceberg.rest.oauth2.token;

import com.auth0.jwt.JWT;
import java.time.Instant;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** A token issued by the authorization server. */
public interface Token {

  /** The raw text of the token. */
  String payload();

  /** The token expiration time as reported in the token response, if any. */
  @Nullable
  Instant expirationTime();

  /**
   * The JWT token expiration time, if the token is a JWT token and contains an expiration claim.
   */
  @Value.Lazy
  @Nullable
  default Instant jwtExpirationTime() {
    try {
      return JWT.decode(payload()).getExpiresAtAsInstant();
    } catch (Exception ignored) {
      return null;
    }
  }

  /**
   * The resolved expiration time of the token, taking into account the token's expiration time and
   * its JWT claims, if applicable.
   */
  @Value.Derived
  @Nullable
  default Instant resolvedExpirationTime() {
    Instant exp = expirationTime();
    return exp != null ? exp : jwtExpirationTime();
  }

  /**
   * Returns true if the token is expired at the given time, inspecting the token's expiration time
   * and its JWT claims, if applicable. Note that if no expiration time is found, this method
   * returns false.
   */
  default boolean expired(Instant when) {
    Instant exp = resolvedExpirationTime();
    return exp != null && !exp.isAfter(when);
  }
}
