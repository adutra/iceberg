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

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.rest.ClientRequest;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;

public abstract class JwtClientAuthenticator implements StandardClientAuthenticator {

  public abstract ClientAssertionConfig clientAssertionConfig();

  public abstract URI tokenEndpoint();

  public abstract Clock clock();

  @Override
  public final <R extends ClientRequest, B extends ClientRequest.Builder<R, B>> void authenticate(
      ClientRequest.Builder<R, B> request,
      Map<String, String> headers,
      @Nullable Tokens currentTokens) {
    Algorithm algorithm = algorithm();
    String jwt = createJwt(algorithm);
    request.clientAssertion(TypedToken.of(jwt, TypedToken.URN_JWT_BEARER));
  }

  protected abstract Algorithm algorithm();

  String createJwt(Algorithm algorithm) {
    Instant now = clock().instant();
    ClientAssertionConfig config = clientAssertionConfig();
    JWTCreator.Builder builder =
        JWT.create()
            .withJWTId(UUID.randomUUID().toString())
            .withIssuer(config.issuer().orElseGet(this::clientId))
            .withSubject(config.issuer().orElseGet(this::clientId))
            .withAudience(config.audience().orElseGet(() -> tokenEndpoint().toString()))
            .withIssuedAt(now)
            .withExpiresAt(now.plus(config.tokenLifespan()));
    config.extraClaims().forEach(builder::withClaim);
    return builder.sign(algorithm);
  }
}
