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
package org.apache.iceberg.rest.auth.oauth2.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.auth.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.rest.MetadataDiscoveryResponse;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.server.MockHttpServer;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;

class TestEndpointProvider {

  private static final String INVALID_METADATA =
      "{"
          + "\"authorization_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/auth\","
          + "\"token_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/token\""
          + "}";

  @Test
  void withoutDiscovery() {
    try (TestEnvironment env = TestEnvironment.builder().discoveryEnabled(false).build()) {
      EndpointProvider endpointProvider = env.endpointProvider();
      assertThat(endpointProvider.resolvedTokenEndpoint()).isEqualTo(env.tokenEndpoint());
      assertThat(endpointProvider.resolvedAuthorizationEndpoint())
          .isEqualTo(env.authorizationEndpoint());
    }
  }

  @Test
  void withDiscovery() {
    try (TestEnvironment env = TestEnvironment.builder().discoveryEnabled(true).build()) {
      EndpointProvider endpointProvider = env.endpointProvider();
      assertThat(endpointProvider.resolvedTokenEndpoint()).isEqualTo(env.tokenEndpoint());
      assertThat(endpointProvider.resolvedAuthorizationEndpoint())
          .isEqualTo(env.authorizationEndpoint());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "''              , /.well-known/openid-configuration",
    "/               , /.well-known/openid-configuration",
    "''              , /.well-known/oauth-authorization-server",
    "/               , /.well-known/oauth-authorization-server",
    "/realms/master  , /realms/master/.well-known/openid-configuration",
    "/realms/master/ , /realms/master/.well-known/openid-configuration",
    "/realms/master  , /realms/master/.well-known/oauth-authorization-server",
    "/realms/master/ , /realms/master/.well-known/oauth-authorization-server"
  })
  void fetchOpenIdProviderMetadataSuccess(String contextPath, String wellKnownPath) {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .authorizationServerContextPath(contextPath)
            .wellKnownPath(wellKnownPath)
            .build()) {
      EndpointProvider endpointProvider = env.endpointProvider();
      MetadataDiscoveryResponse actual = endpointProvider.openIdProviderMetadata();
      assertThat(actual.issuerUrl()).isEqualTo(env.authorizationServerUrl());
      assertThat(actual.tokenEndpoint()).isEqualTo(env.tokenEndpoint());
      assertThat(actual.authorizationEndpoint()).isEqualTo(env.authorizationEndpoint());
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongEndpoint() {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {
      env.createErrorExpectations();
      EndpointProvider endpointProvider = env.endpointProvider();
      Throwable error = catchThrowable(endpointProvider::openIdProviderMetadata);
      assertThat(error)
          .isInstanceOf(RESTException.class)
          .hasMessageContaining("Failed to fetch OpenID provider metadata");
      // first well-known path
      assertThat(error.getCause())
          .asInstanceOf(throwable(OAuth2Exception.class))
          .hasMessageContaining("OAuth2 request failed: Invalid request")
          .extracting(OAuth2Exception::errorResponse)
          .extracting(ErrorResponse::type, ErrorResponse::code, ErrorResponse::message)
          .containsExactly("invalid_request", 401, "Invalid request");
      // second well-known path
      assertThat(error.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(OAuth2Exception.class))
          .hasMessageContaining("OAuth2 request failed: Invalid request")
          .extracting(OAuth2Exception::errorResponse)
          .extracting(ErrorResponse::type, ErrorResponse::code, ErrorResponse::message)
          .containsExactly("invalid_request", 401, "Invalid request");
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongData() {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {
      ((MockHttpServer) env.server())
          .getClientAndServer()
          .when(HttpRequest.request())
          .respond(
              HttpResponse.response()
                  .withStatusCode(200)
                  .withContentType(MediaType.APPLICATION_JSON)
                  .withBody(JsonBody.json(INVALID_METADATA)));
      EndpointProvider endpointProvider = env.endpointProvider();
      Throwable error = catchThrowable(endpointProvider::openIdProviderMetadata);
      // first well-known path
      assertThat(error)
          .isInstanceOf(RESTException.class)
          .hasMessageContaining("Failed to fetch OpenID provider metadata");
      assertThat(error.getCause())
          .asInstanceOf(throwable(IllegalArgumentException.class))
          .hasMessageContaining("Cannot parse missing string: issuer");
      // second well-known path
      assertThat(error.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(IllegalArgumentException.class))
          .hasMessageContaining("Cannot parse missing string: issuer");
    }
  }
}
