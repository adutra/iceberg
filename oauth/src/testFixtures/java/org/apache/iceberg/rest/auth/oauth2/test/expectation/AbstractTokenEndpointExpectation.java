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

import java.net.URI;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableDefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenResponse;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

@SuppressWarnings("resource")
public abstract class AbstractTokenEndpointExpectation extends AbstractExpectation {

  protected HttpRequest tokenRequestTemplate() {
    URI tokenEndpoint = testEnvironment().tokenEndpoint();
    String path = tokenEndpoint.getPath();
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
    request.withHeader(
        "Authorization", String.format("Basic %s", TestConstants.CLIENT_CREDENTIALS1_BASE_64));
  }

  protected abstract PostFormRequest tokenRequestBody();

  protected HttpResponse tokenResponse() {
    return HttpResponse.response().withBody(ExpectationUtils.jsonBody(tokenResponseBody()));
  }

  protected TokenResponse tokenResponseBody() {
    DefaultTokenResponse.Builder builder = ImmutableDefaultTokenResponse.builder();
    return buildTokenResponseBody(builder).build();
  }

  protected TokenResponse.Builder<?, ?> buildTokenResponseBody(
      TokenResponse.Builder<?, ?> builder) {
    return builder
        .accessTokenPayload("access_initial")
        .accessTokenExpiresInSeconds((int) TestConstants.ACCESS_TOKEN_LIFESPAN.toSeconds())
        .tokenType("bearer");
  }
}
