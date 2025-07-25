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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

import com.nimbusds.oauth2.sdk.ParseException;
import org.apache.iceberg.rest.auth.oauth2.http.HttpClient;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;

class TestEndpointProvider {

  private static final String INVALID_METADATA = "{\"authorization_endpoint\":\" invalid \"}";

  @Test
  void withoutDiscovery() {
    try (TestEnvironment env = TestEnvironment.builder().discoveryEnabled(false).build()) {
      EndpointProvider endpointProvider = EndpointProvider.create(env.config(), HttpClient.DEFAULT);
      assertThat(endpointProvider.resolvedTokenEndpoint()).isEqualTo(env.tokenEndpoint());
      assertThat(endpointProvider.resolvedAuthorizationEndpoint())
          .isEqualTo(env.authorizationEndpoint());
      assertThat(endpointProvider.resolvedDeviceAuthorizationEndpoint())
          .isEqualTo(env.deviceAuthorizationEndpoint());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void withDiscovery(boolean includeDeviceAuthEndpoint) {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .includeDeviceAuthEndpointInDiscoveryMetadata(includeDeviceAuthEndpoint)
            .build()) {
      EndpointProvider endpointProvider = EndpointProvider.create(env.config(), HttpClient.DEFAULT);
      assertThat(endpointProvider.resolvedTokenEndpoint()).isEqualTo(env.tokenEndpoint());
      assertThat(endpointProvider.resolvedAuthorizationEndpoint())
          .isEqualTo(env.authorizationEndpoint());
      if (includeDeviceAuthEndpoint) {
        assertThat(endpointProvider.resolvedDeviceAuthorizationEndpoint())
            .isEqualTo(env.deviceAuthorizationEndpoint());
      } else {
        assertThatThrownBy(endpointProvider::resolvedDeviceAuthorizationEndpoint)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(
                "OpenID provider metadata does not contain a device authorization endpoint");
      }
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
      EndpointProvider endpointProvider = EndpointProvider.create(env.config(), HttpClient.DEFAULT);
      var actual = endpointProvider.openIdProviderMetadata();
      assertThat(actual.getTokenEndpointURI()).isEqualTo(env.tokenEndpoint());
      assertThat(actual.getAuthorizationEndpointURI()).isEqualTo(env.authorizationEndpoint());
      assertThat(actual.getDeviceAuthorizationEndpointURI())
          .isEqualTo(env.deviceAuthorizationEndpoint());
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongEndpoint() {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {
      env.createErrorExpectations();
      EndpointProvider endpointProvider = EndpointProvider.create(env.config(), HttpClient.DEFAULT);
      Throwable throwable = catchThrowable(endpointProvider::openIdProviderMetadata);
      assertThat(throwable)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to fetch provider metadata");
      // OIDC well-known path
      assertThat(throwable.getCause())
          .asInstanceOf(throwable(RuntimeException.class))
          .hasMessageContaining("Failed to fetch OIDC provider metadata")
          .hasMessageContaining("Invalid request");
      // OAuth well-known path
      assertThat(throwable.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(RuntimeException.class))
          .hasMessageContaining("Failed to fetch OAuth provider metadata")
          .hasMessageContaining("Invalid request");
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongData() {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {
      @SuppressWarnings("resource")
      ClientAndServer server = env.mockServer().orElseThrow();
      server
          .when(HttpRequest.request())
          .respond(
              HttpResponse.response()
                  .withStatusCode(200)
                  .withContentType(MediaType.APPLICATION_JSON)
                  .withBody(JsonBody.json(INVALID_METADATA)));
      EndpointProvider endpointProvider = EndpointProvider.create(env.config(), HttpClient.DEFAULT);
      Throwable throwable = catchThrowable(endpointProvider::openIdProviderMetadata);
      // OIDC well-known path
      assertThat(throwable)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to fetch provider metadata");
      assertThat(throwable.getCause())
          .asInstanceOf(throwable(ParseException.class))
          .hasMessageContaining("Illegal character in path at index 0");
      // OAuth well-known path
      assertThat(throwable.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(ParseException.class))
          .hasMessageContaining("Illegal character in path at index 0");
    }
  }
}
