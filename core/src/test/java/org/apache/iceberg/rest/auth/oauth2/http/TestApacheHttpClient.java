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
package org.apache.iceberg.rest.auth.oauth2.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.ReadOnlyHTTPResponse;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableHttpClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockserver.integration.ClientAndServer;

class TestApacheHttpClient {

  private static ClientAndServer mockServer;
  private static String baseUrl;

  @BeforeAll
  static void beforeClass() {
    mockServer = ClientAndServer.startClientAndServer();
    baseUrl = "http://localhost:" + mockServer.getLocalPort();
  }

  @AfterAll
  static void afterClass() {
    if (mockServer != null) {
      mockServer.close();
    }
  }

  @AfterEach
  void after() {
    mockServer.reset();
  }

  @Test
  void testDefaultConstructor() throws IOException {
    mockServer
        .when(request().withMethod("GET").withPath("/test"))
        .respond(response().withStatusCode(200).withBody("success"));
    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testSendRequestWithHttpMethods() throws IOException {
    for (HTTPRequest.Method method :
        List.of(
            HTTPRequest.Method.GET,
            HTTPRequest.Method.POST,
            HTTPRequest.Method.PUT,
            HTTPRequest.Method.DELETE)) {
      mockServer.reset();
      mockServer
          .when(request().withMethod(method.name()).withPath("/test"))
          .respond(response().withStatusCode(200).withBody("success"));
      try (ApacheHttpClient client = new ApacheHttpClient()) {
        HTTPRequest httpRequest = new HTTPRequest(method, URI.create(baseUrl + "/test"));
        ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
        assertThat(httpResponse.getStatusCode()).isEqualTo(200);
        assertThat(httpResponse.getBody()).isEqualTo("success");
      }
    }
  }

  @Test
  void testSendRequestWithBody() throws IOException {
    mockServer
        .when(
            request()
                .withMethod("POST")
                .withPath("/token")
                .withBody("{\"grant_type\":\"client_credentials\"}"))
        .respond(
            response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"access_token\":\"test_token\"}"));
    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.POST, URI.create(baseUrl + "/token"));
      httpRequest.setBody("{\"grant_type\":\"client_credentials\"}");
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).contains("access_token").contains("test_token");
    }
  }

  @Test
  void testSendRequestWithHeaders() throws IOException {
    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/test")
                .withHeader("X-Header-1", "value1")
                .withHeader("X-Header-2", "value2"))
        .respond(response().withStatusCode(200).withBody("success"));
    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test"));
      httpRequest.setHeader("X-Header-1", "value1");
      httpRequest.setHeader("X-Header-2", "value2");
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testSendRequestWithQueryParameters() throws IOException {
    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/test")
                .withQueryStringParameter("param1", "value1"))
        .respond(response().withStatusCode(200).withBody("success"));
    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test?param1=value1"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testSendRequestWithFormData() throws Exception {
    mockServer
        .when(
            request()
                .withMethod("POST")
                .withPath("/test")
                .withHeader("Content-Type", "application/x-www-form-urlencoded"))
        .respond(response().withStatusCode(200).withBody("success"));
    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.POST, URI.create(baseUrl + "/test"));
      httpRequest.setContentType("application/x-www-form-urlencoded");
      httpRequest.setBody("grant_type=client_credentials");
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testSendRequestWithErrorResponse() throws IOException {
    mockServer
        .when(request().withMethod("GET").withPath("/error"))
        .respond(
            response()
                .withStatusCode(400)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"error\":\"invalid_request\"}"));
    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/error"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(400);
      assertThat(httpResponse.getBody()).contains("error").contains("invalid_request");
    }
  }

  @Test
  void testConfigWithMultipleHeaders() throws IOException {
    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/test")
                .withHeader("X-Header-1", "value1")
                .withHeader("X-Header-2", "value2"))
        .respond(response().withStatusCode(200).withBody("success"));

    HttpClientConfig config =
        ImmutableHttpClientConfig.builder()
            .putHeaders("X-Header-1", "value1")
            .putHeaders("X-Header-2", "value2")
            .build();

    try (ApacheHttpClient client = new ApacheHttpClient(config)) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testConfigWithTimeouts() throws IOException {
    mockServer
        .when(request().withMethod("GET").withPath("/test"))
        .respond(response().withStatusCode(200).withBody("success"));

    HttpClientConfig config =
        ImmutableHttpClientConfig.builder()
            .readTimeout(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(10))
            .build();

    try (ApacheHttpClient client = new ApacheHttpClient(config)) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @CartesianTest
  void testConfigWithCompression(@Values(booleans = {true, false}) boolean compressionEnabled)
      throws IOException {
    mockServer
        .when(request().withMethod("GET").withPath("/test"))
        .respond(response().withStatusCode(200).withBody("success"));

    HttpClientConfig config =
        ImmutableHttpClientConfig.builder().compressionEnabled(compressionEnabled).build();

    try (ApacheHttpClient client = new ApacheHttpClient(config)) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testConfigWithSsl() throws IOException {
    mockServer
        .when(request().withMethod("GET").withPath("/test"))
        .respond(response().withStatusCode(200).withBody("success"));

    HttpClientConfig config =
        ImmutableHttpClientConfig.builder()
            .sslProtocols(List.of("TLSv1.3", "TLSv1.2"))
            .sslCipherSuites(
                List.of(
                    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"))
            .sslTrustAll(true)
            .sslHostnameVerificationEnabled(false)
            .build();

    try (ApacheHttpClient client = new ApacheHttpClient(config)) {
      String httpsUrl = "https://localhost:" + mockServer.getLocalPort();
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(httpsUrl + "/test"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).isEqualTo("success");
    }
  }

  @Test
  void testResponseHeaders() throws IOException {
    mockServer
        .when(request().withMethod("GET").withPath("/test"))
        .respond(
            response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withHeader("X-Custom-Header", "custom-value")
                .withBody("{\"status\":\"ok\"}"));

    try (ApacheHttpClient client = new ApacheHttpClient()) {
      HTTPRequest httpRequest =
          new HTTPRequest(HTTPRequest.Method.GET, URI.create(baseUrl + "/test"));
      ReadOnlyHTTPResponse httpResponse = client.send(httpRequest);
      assertThat(httpResponse.getStatusCode()).isEqualTo(200);
      assertThat(httpResponse.getBody()).contains("status").contains("ok");
      assertThat(httpResponse.getHeaderMap()).containsKey("Content-Type");
      assertThat(httpResponse.getHeaderMap()).containsKey("X-Custom-Header");
      assertThat(httpResponse.getHeaderMap().get("Content-Type")).contains("application/json");
      assertThat(httpResponse.getHeaderMap().get("X-Custom-Header")).contains("custom-value");
    }
  }
}
