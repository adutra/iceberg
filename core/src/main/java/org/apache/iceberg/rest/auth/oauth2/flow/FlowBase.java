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
package org.apache.iceberg.rest.auth.oauth2.flow;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.assertions.jwt.JWTAssertionDetails;
import com.nimbusds.oauth2.sdk.assertions.jwt.JWTAssertionFactory;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretJWT;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.PrivateKeyJWT;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPRequestSender;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.JWTID;
import com.nimbusds.oauth2.sdk.id.Subject;
import java.net.URI;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2ClientRuntime;
import org.apache.iceberg.rest.auth.oauth2.config.ClientAssertionConfig;
import org.apache.iceberg.rest.auth.oauth2.crypto.PemReader;
import org.apache.iceberg.rest.auth.oauth2.endpoint.EndpointProvider;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Infrastructure shared by all flows. */
abstract class FlowBase implements Flow {

  static final String OAUTH2_CLIENT_TITLE = "======== Authentication Required ========";
  static final String OAUTH2_CLIENT_OPEN_URL = "Please open the following URL to continue:";

  private static final Logger LOGGER = LoggerFactory.getLogger(FlowBase.class);

  static String contextPath(String clientName) {
    return '/' + clientName + "/auth";
  }

  static String msgPrefix(String clientName) {
    return '[' + clientName + "] ";
  }

  abstract OAuth2Config config();

  abstract OAuth2ClientRuntime runtime();

  abstract HTTPRequestSender requestSender();

  abstract EndpointProvider endpointProvider();

  @Value.Derived
  String clientName() {
    return config().basicConfig().clientName().orElse(OAuth2Client.DEFAULT_CLIENT_NAME);
  }

  interface Builder<F extends FlowBase, B extends Builder<F, B>> {

    @CanIgnoreReturnValue
    B config(OAuth2Config config);

    @CanIgnoreReturnValue
    B runtime(OAuth2ClientRuntime runtime);

    @CanIgnoreReturnValue
    B requestSender(HTTPRequestSender requestSender);

    @CanIgnoreReturnValue
    B endpointProvider(EndpointProvider endpointProvider);

    F build();
  }

  CompletionStage<TokensResult> invokeTokenEndpoint(AuthorizationGrant grant) {
    HTTPRequest request;
    try {
      TokenRequest.Builder builder = newTokenRequestBuilder(grant);
      request = builder.build().toHTTPRequest();
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.supplyAsync(() -> sendAndReceive(request), runtime().executor())
        .whenComplete((response, error) -> log(request, response, error))
        .thenApply(this::parseTokenResponse)
        .thenApply(this::toTokensResult);
  }

  TokenRequest.Builder newTokenRequestBuilder(AuthorizationGrant grant) {
    URI tokenEndpoint = endpointProvider().resolvedTokenEndpoint();
    TokenRequest.Builder builder =
        publicClient()
            ? new TokenRequest.Builder(tokenEndpoint, clientId(), grant)
            : new TokenRequest.Builder(tokenEndpoint, createClientAuthentication(), grant);
    config().basicConfig().scope().ifPresent(builder::scope);
    config().basicConfig().extraRequestParameters().forEach(builder::customParameter);
    return builder;
  }

  HTTPResponse sendAndReceive(HTTPRequest request) {
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[{}] Invoking endpoint: {}", clientName(), request.getURI());
      }

      return request.send(requestSender());
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke endpoint: " + request.getURI(), e);
    }
  }

  AccessTokenResponse parseTokenResponse(HTTPResponse httpResponse) {
    try {
      TokenResponse response = TokenResponse.parse(httpResponse);
      if (!response.indicatesSuccess()) {
        TokenErrorResponse errorResponse = response.toErrorResponse();
        throw new OAuth2Exception(errorResponse);
      }

      return response.toSuccessResponse();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  TokensResult toTokensResult(AccessTokenResponse response) {
    Instant now = runtime().clock().instant();
    return TokensResult.of(response, now);
  }

  void log(HTTPRequest request, HTTPResponse response, Throwable error) {
    if (error == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "[{}] Received {} response from endpoint: {}",
            clientName(),
            response.getStatusCode(),
            request.getURI());
      }
    } else {
      LOGGER.warn("[{}] Error invoking endpoint: {}", clientName(), request.getURI(), error);
    }
  }

  boolean publicClient() {
    return config()
        .basicConfig()
        .clientAuthenticationMethod()
        .equals(ClientAuthenticationMethod.NONE);
  }

  ClientID clientId() {
    return config()
        .basicConfig()
        .clientId()
        .orElseThrow(() -> new IllegalStateException("Client ID is required"));
  }

  Secret clientSecret() {
    return config()
        .basicConfig()
        .clientSecret()
        .orElseThrow(() -> new IllegalStateException("Client secret is required"));
  }

  ClientAuthentication createClientAuthentication() {
    URI tokenEndpoint = endpointProvider().resolvedTokenEndpoint();

    ClientAuthenticationMethod method = config().basicConfig().clientAuthenticationMethod();

    if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)) {
      return new ClientSecretBasic(clientId(), clientSecret());

    } else if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
      return new ClientSecretPost(clientId(), clientSecret());

    } else if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_JWT)) {
      JWTAssertionDetails details = createJwtAssertionDetails(tokenEndpoint);
      JWSAlgorithm algorithm =
          config().clientAssertionConfig().algorithm().orElse(JWSAlgorithm.HS256);
      try {
        SignedJWT assertion = JWTAssertionFactory.create(details, algorithm, clientSecret());
        return new ClientSecretJWT(assertion);
      } catch (JOSEException e) {
        throw new RuntimeException(e);
      }

    } else if (method.equals(ClientAuthenticationMethod.PRIVATE_KEY_JWT)) {
      JWTAssertionDetails details = createJwtAssertionDetails(tokenEndpoint);
      JWSAlgorithm algorithm =
          config().clientAssertionConfig().algorithm().orElse(JWSAlgorithm.RS256);
      Path privateKeyPath =
          config()
              .clientAssertionConfig()
              .privateKey()
              .orElseThrow(() -> new IllegalStateException("Private key is required"));
      PrivateKey privateKey = PemReader.instance().readPrivateKey(privateKeyPath);
      try {
        String kid = config().clientAssertionConfig().keyId().orElse(null);
        SignedJWT assertion =
            JWTAssertionFactory.create(details, algorithm, privateKey, kid, null, null, null);
        return new PrivateKeyJWT(assertion);
      } catch (JOSEException e) {
        throw new RuntimeException(e);
      }
    }

    throw new IllegalArgumentException("Unsupported client authentication method: " + method);
  }

  private JWTAssertionDetails createJwtAssertionDetails(URI tokenEndpoint) {
    ClientAssertionConfig assertionConfig = config().clientAssertionConfig();
    Issuer issuer = assertionConfig.issuer().orElseGet(() -> new Issuer(clientId().getValue()));
    Subject subject = assertionConfig.subject().orElseGet(() -> new Subject(clientId().getValue()));
    List<Audience> audiences =
        assertionConfig.audiences().isEmpty()
            ? List.of(new Audience(tokenEndpoint))
            : assertionConfig.audiences();
    Instant issuedAt = runtime().clock().instant();
    Instant expiration = issuedAt.plus(assertionConfig.tokenLifespan());
    @SuppressWarnings({"rawtypes", "unchecked"})
    Map<String, Object> extraClaims = (Map) assertionConfig.extraClaims();
    return new JWTAssertionDetails(
        issuer,
        subject,
        audiences,
        Date.from(expiration),
        Date.from(issuedAt),
        Date.from(issuedAt),
        new JWTID(),
        extraClaims);
  }
}
