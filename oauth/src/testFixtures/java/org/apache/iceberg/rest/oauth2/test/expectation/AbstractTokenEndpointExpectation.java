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
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.rest.ImmutableTokenResponse;
import org.apache.iceberg.rest.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.apache.iceberg.rest.oauth2.token.TypedToken;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

@SuppressWarnings("resource")
public abstract class AbstractTokenEndpointExpectation extends AbstractExpectation {

  protected HttpRequest tokenRequestTemplate() {
    URI tokenEndpoint = testEnvironment().tokenEndpoint();
    String path =
        tokenEndpoint.isAbsolute()
            ? tokenEndpoint.getPath()
            : testEnvironment().catalogServerContextPath() + tokenEndpoint.getPath();
    return HttpRequest.request()
        .withMethod("POST")
        .withPath(path)
        .withHeader("Content-Type", "application/x-www-form-urlencoded")
        .withHeader("Accept", "application/json");
  }

  protected HttpRequest tokenRequest() {
    HttpRequest request =
        tokenRequestTemplate().withBody(ExpectationUtils.parameterBody(tokenRequestBody()));
    addRequestHeaders(request);
    return request;
  }

  protected void addRequestHeaders(HttpRequest request) {
    if (testEnvironment().privateClient()) {
      request.withHeader(
          "Authorization",
          String.format(
              "Basic (%s|%s)",
              TestConstants.CLIENT_CREDENTIALS1_BASE_64,
              TestConstants.CLIENT_CREDENTIALS2_BASE_64));
    }
  }

  protected abstract PostFormRequest tokenRequestBody();

  protected HttpResponse tokenResponse(
      HttpRequest httpRequest, String accessToken, String refreshToken) {
    return HttpResponse.response()
        .withBody(ExpectationUtils.jsonBody(tokenResponseBody(accessToken, refreshToken).build()));
  }

  protected ImmutableTokenResponse.Builder tokenResponseBody(
      String accessToken, String refreshToken) {
    ImmutableTokenResponse.Builder responseBody =
        ImmutableTokenResponse.builder()
            .accessTokenPayload(accessToken)
            .accessTokenExpiresInSeconds((int) testEnvironment().accessTokenLifespan().toSeconds())
            .tokenType("bearer");
    if (testEnvironment().returnRefreshTokens()
        && testEnvironment().grantType() != GrantType.CLIENT_CREDENTIALS) {
      responseBody
          .refreshTokenPayload(refreshToken)
          .refreshTokenExpiresInSeconds((int) testEnvironment().refreshTokenLifespan().toSeconds());
    }

    if (testEnvironment().grantType() == GrantType.TOKEN_EXCHANGE) {
      // included for completeness, but not used
      responseBody.issuedTokenType(TypedToken.URN_ACCESS_TOKEN);
    }

    return responseBody;
  }
}
