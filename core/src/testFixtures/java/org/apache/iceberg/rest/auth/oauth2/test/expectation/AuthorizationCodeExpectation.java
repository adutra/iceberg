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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.pkce.CodeChallenge;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.apache.http.client.utils.URIBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameters;

@Value.Immutable
@Value.Enclosing
@SuppressWarnings("resource")
public abstract class AuthorizationCodeExpectation extends TokenEndpointExpectation {

  /** A map of pending authorization requests, keyed by the redirect URI. */
  @Value.Lazy
  protected ConcurrentMap<String, PendingAuthRequest> pendingAuthRequests() {
    return Maps.newConcurrentMap();
  }

  @Override
  public void create() {
    createAuthEndpointExpectation();
    mockServer()
        .when(request())
        .respond(httpRequest -> response(httpRequest, "access_initial", "refresh_initial"));
  }

  @Override
  protected ImmutableMap.Builder<String, String> requestBody() {
    ImmutableMap.Builder<String, String> builder =
        super.requestBody()
            .put("grant_type", GrantType.AUTHORIZATION_CODE.toString())
            .put("code", "[a-zA-Z0-9-._~]+")
            .put("redirect_uri", "https?://.*");
    if (testEnvironment().pkceEnabled()) {
      builder.put("code_verifier", "[a-zA-Z0-9-._~]+");
    }

    return builder;
  }

  @Override
  protected HttpResponse response(
      HttpRequest httpRequest, String accessToken, String refreshToken) {
    Map<String, List<String>> params = decodeBodyParameters(httpRequest);
    String redirectUri = params.get("redirect_uri").get(0);
    PendingAuthRequest pendingAuthRequest = pendingAuthRequests().get(redirectUri);
    if (pendingAuthRequest == null) {
      return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
    }

    List<String> code = params.get("code");
    if (code == null
        || code.isEmpty()
        || !code.get(0).equals(pendingAuthRequest.code().getValue())) {
      return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
    }

    if (testEnvironment().pkceEnabled()) {
      if (pendingAuthRequest.codeChallengeMethod().isEmpty()
          || pendingAuthRequest.codeChallenge().isEmpty()) {
        return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
      }

      List<String> codeVerifier = params.get("code_verifier");
      if (codeVerifier == null || codeVerifier.isEmpty()) {
        return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
      }

      if (!pendingAuthRequest
          .codeChallenge()
          .get()
          .equals(pendingAuthRequest.codeChallenge().get())) {
        return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
      }
    }

    pendingAuthRequests().remove(redirectUri);
    return super.response(httpRequest, accessToken, refreshToken);
  }

  private void createAuthEndpointExpectation() {
    HttpRequest request =
        HttpRequest.request()
            .withMethod("GET")
            .withPath(testEnvironment().authorizationEndpoint().getPath())
            .withQueryStringParameter("response_type", "code")
            .withQueryStringParameter("client_id", ACCEPTED_CLIENT_IDS)
            .withQueryStringParameter("scope", ACCEPTED_SCOPES)
            .withQueryStringParameter(
                "redirect_uri", "https?://localhost:\\d+/iceberg-oauth2-client-\\d+(-\\w+)?/auth")
            .withQueryStringParameter("state", "[a-zA-Z0-9-._~]+")
            .withQueryStringParameter(ACCEPTED_EXTRA_PARAM_NAMES, ACCEPTED_EXTRA_PARAM_VALUES);
    if (testEnvironment().pkceEnabled()) {
      request.withQueryStringParameter("code_challenge", "[a-zA-Z0-9-._~]+");
      request.withQueryStringParameter(
          "code_challenge_method", testEnvironment().codeChallengeMethod().getValue());
    }

    mockServer()
        .when(request)
        .respond(
            httpRequest -> {
              Parameters parameters = httpRequest.getQueryStringParameters();
              String redirectUri = parameters.getValues("redirect_uri").get(0);
              AuthorizationCode code = new AuthorizationCode();
              String location =
                  new URIBuilder(redirectUri)
                      .addParameter("code", code.getValue())
                      .addParameter("state", parameters.getValues("state").get(0))
                      .build()
                      .toString();
              CodeChallengeMethod method = null;
              CodeChallenge codeChallenge = null;
              if (testEnvironment().pkceEnabled()) {
                if (parameters.getValues("code_challenge_method").isEmpty()
                    || parameters.getValues("code_challenge").isEmpty()) {
                  return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
                }

                method =
                    CodeChallengeMethod.parse(parameters.getValues("code_challenge_method").get(0));
                codeChallenge = CodeChallenge.parse(parameters.getValues("code_challenge").get(0));
              }

              var pendingAuthRequest =
                  ImmutableAuthorizationCodeExpectation.PendingAuthRequest.builder()
                      .code(code)
                      .codeChallengeMethod(Optional.ofNullable(method))
                      .codeChallenge(Optional.ofNullable(codeChallenge))
                      .build();
              pendingAuthRequests().put(redirectUri, pendingAuthRequest);
              return HttpResponse.response().withStatusCode(302).withHeader("Location", location);
            });
  }

  @Value.Immutable
  public interface PendingAuthRequest {

    AuthorizationCode code();

    Optional<CodeChallengeMethod> codeChallengeMethod();

    Optional<CodeChallenge> codeChallenge();
  }
}
