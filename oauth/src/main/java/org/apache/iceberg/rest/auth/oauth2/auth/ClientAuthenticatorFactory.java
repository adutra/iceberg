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
package org.apache.iceberg.rest.auth.oauth2.auth;

import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.Dialect;

public final class ClientAuthenticatorFactory {

  private ClientAuthenticatorFactory() {}

  public static ClientAuthenticator createAuthenticator(BasicConfig spec) {
    if (spec.dialect() == Dialect.ICEBERG_REST || spec.token().isPresent()) {
      return ImmutableIcebergClientAuthenticator.builder()
          .clientId(spec.clientId())
          .clientSecret(spec.clientSecret())
          .build();
    } else {
      ClientAuthentication method = spec.clientAuthentication();
      switch (method) {
        case NONE:
          return ImmutablePublicClientAuthenticator.builder()
              .clientId(spec.clientId().orElseThrow())
              .build();
        case CLIENT_SECRET_BASIC:
          return ImmutableClientSecretBasicAuthenticator.builder()
              .clientId(spec.clientId().orElseThrow())
              .clientSecret(spec.clientSecret().orElseThrow())
              .build();
        case CLIENT_SECRET_POST:
          return ImmutableClientSecretPostAuthenticator.builder()
              .clientId(spec.clientId().orElseThrow())
              .clientSecret(spec.clientSecret().orElseThrow())
              .build();
        default:
          throw new IllegalArgumentException("Unsupported client authentication method: " + method);
      }
    }
  }
}
