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

import com.auth0.jwt.algorithms.Algorithm;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public abstract class ClientSecretJwtAuthenticator extends JwtClientAuthenticator
    implements ClientSecretAuthenticator {

  public static final JwtSigningAlgorithm DEFAULT_ALGORITHM = JwtSigningAlgorithm.HMAC_SHA512;

  @Override
  protected Algorithm algorithm() {
    JwtSigningAlgorithm algorithm = clientAssertionConfig().algorithm().orElse(DEFAULT_ALGORITHM);
    return algorithm.getHmacAlgorithm(clientSecret().value());
  }
}
