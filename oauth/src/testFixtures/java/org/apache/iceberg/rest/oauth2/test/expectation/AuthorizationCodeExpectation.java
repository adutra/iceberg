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
package org.apache.iceberg.rest.oauth2.test.expectation;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.oauth2.config.PkceTransformation;
import org.apache.iceberg.rest.oauth2.flow.FlowUtils;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.rest.ImmutableAuthorizationCodeTokenRequest;
import org.apache.iceberg.rest.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameters;

@Value.Immutable
@OAuth2ImmutableStyle
@Value.Enclosing
@SuppressWarnings("resource")
public abstract class AuthorizationCodeExpectation extends InitialTokenFetchExpectation {

  /** A map of pending authorization requests, keyed by the redirect URI. */
  @Value.Lazy
  protected ConcurrentMap<String, PendingAuthRequest> pendingAuthRequests() {
    return Maps.newConcurrentMap();
  }

  @Override
  public void create() {
    createAuthEndpointExpectation();
    clientAndServer().when(tokenRequest()).respond(this::tokenResponse);
  }

  @Override
  protected PostFormRequest tokenRequestBody() {
    return ImmutableAuthorizationCodeTokenRequest.builder()
        .clientId(
            testEnvironment().privateClient()
                ? null
                : String.format("(%s|%s)", TestConstants.CLIENT_ID1, TestConstants.CLIENT_ID2))
        .code("\\w{4}-\\w{4}")
        .redirectUri(URI.create("http://.*"))
        .scope(String.format("(%s|%s)", TestConstants.SCOPE1, TestConstants.SCOPE2))
        .putExtraParameter("(extra1|extra2)", "(value1|value2)")
        .build();
  }

  private HttpResponse tokenResponse(HttpRequest httpRequest) {
    List<NameValuePair> params = ExpectationUtils.decodeBodyParameters(httpRequest);
    String redirectUri = ExpectationUtils.findFirstParameterByName(params, "redirect_uri");
    PendingAuthRequest pendingAuthRequest = pendingAuthRequests().get(redirectUri);
    if (pendingAuthRequest == null) {
      return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
    }

    String code = ExpectationUtils.findFirstParameterByName(params, "code");
    if (code == null || code.isEmpty()) {
      return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
    }

    if (testEnvironment().pkceEnabled()) {
      if (pendingAuthRequest.pkceTransformation().isEmpty()
          || pendingAuthRequest.pkceCodeChallenge().isEmpty()) {
        return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
      }

      String codeVerifier = ExpectationUtils.findFirstParameterByName(params, "code_verifier");
      if (codeVerifier == null || codeVerifier.isEmpty()) {
        return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
      }

      String expectedCodeChallenge =
          FlowUtils.generateCodeChallenge(
              pendingAuthRequest.pkceTransformation().get(), codeVerifier);
      if (!pendingAuthRequest.pkceCodeChallenge().get().equals(expectedCodeChallenge)) {
        return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
      }
    }

    pendingAuthRequests().remove(redirectUri);
    return super.tokenResponse("access_initial", "refresh_initial");
  }

  private void createAuthEndpointExpectation() {
    HttpRequest request =
        HttpRequest.request()
            .withMethod("GET")
            .withPath(testEnvironment().authorizationEndpoint().getPath())
            .withQueryStringParameter("response_type", "code")
            .withQueryStringParameter(
                "client_id",
                String.format("(%s|%s)", TestConstants.CLIENT_ID1, TestConstants.CLIENT_ID2))
            .withQueryStringParameter(
                "scope", String.format("(%s|%s)", TestConstants.SCOPE1, TestConstants.SCOPE2))
            .withQueryStringParameter(
                "redirect_uri", "http://localhost:\\d+/iceberg-auth-manager-\\w+(-\\w+)?/auth")
            .withQueryStringParameter("state", "\\w+");
    if (testEnvironment().pkceEnabled()) {
      request.withQueryStringParameter("code_challenge", "[a-zA-Z0-9-._~]+");
      request.withQueryStringParameter(
          "code_challenge_method", testEnvironment().pkceTransformation().canonicalName());
    }

    clientAndServer()
        .when(request)
        .respond(
            httpRequest -> {
              Parameters parameters = httpRequest.getQueryStringParameters();
              String redirectUri = parameters.getValues("redirect_uri").get(0);
              String code =
                  FlowUtils.randomAlphaNumString(4) + "-" + FlowUtils.randomAlphaNumString(4);
              String location =
                  new URIBuilder(redirectUri)
                      .addParameter("code", code)
                      .addParameter("state", parameters.getValues("state").get(0))
                      .build()
                      .toString();
              PkceTransformation pkceTransformation = null;
              String codeChallenge = null;
              if (testEnvironment().pkceEnabled()) {
                if (parameters.getValues("code_challenge_method").isEmpty()
                    || parameters.getValues("code_challenge").isEmpty()) {
                  return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
                }

                pkceTransformation =
                    PkceTransformation.fromConfigName(
                        parameters.getValues("code_challenge_method").get(0));
                codeChallenge = parameters.getValues("code_challenge").get(0);
              }

              var pendingAuthRequest =
                  ImmutableAuthorizationCodeExpectation.PendingAuthRequest.builder()
                      .code(code)
                      .pkceTransformation(Optional.ofNullable(pkceTransformation))
                      .pkceCodeChallenge(Optional.ofNullable(codeChallenge))
                      .build();
              pendingAuthRequests().put(redirectUri, pendingAuthRequest);
              return HttpResponse.response().withStatusCode(302).withHeader("Location", location);
            });
  }

  @Value.Immutable
  @OAuth2ImmutableStyle
  public interface PendingAuthRequest {

    String code();

    Optional<PkceTransformation> pkceTransformation();

    Optional<String> pkceCodeChallenge();
  }
}
