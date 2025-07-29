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
package org.apache.iceberg.rest.auth.oauth2.rest.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.rest.AuthorizationCodeTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableAuthorizationCodeTokenRequest;
import org.junit.jupiter.api.Test;

public class TestAuthorizationCodeTokenRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> AuthorizationCodeTokenRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse authorization code token request from null object");

    assertThatThrownBy(() -> AuthorizationCodeTokenRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid authorization code token request: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> AuthorizationCodeTokenRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: grant_type");

    assertThatThrownBy(
            () ->
                AuthorizationCodeTokenRequestParser.fromJson(
                    "{\"grant_type\": \"authorization_code\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: code");

    assertThatThrownBy(
            () ->
                AuthorizationCodeTokenRequestParser.fromJson(
                    "{\"grant_type\": \"authorization_code\", \"code\": \"test-code\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: redirect_uri");
  }

  @Test
  public void invalidGrantType() {
    assertThatThrownBy(
            () ->
                AuthorizationCodeTokenRequestParser.fromJson("{\"grant_type\": \"invalid_grant\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid_grant");

    assertThatThrownBy(
            () ->
                AuthorizationCodeTokenRequestParser.fromJson(
                    "{\"grant_type\": \"client_credentials\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid grant type: client_credentials (expected authorization_code)");

    assertThatThrownBy(() -> AuthorizationCodeTokenRequestParser.fromJson("{\"grant_type\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: grant_type: 123");
  }

  @Test
  public void invalidCode() {
    assertThatThrownBy(
            () ->
                AuthorizationCodeTokenRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"authorization_code\",\n"
                        + "  \"code\" : 123,\n"
                        + "  \"redirect_uri\" : \"https://example.com/callback\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: code: 123");
  }

  @Test
  public void invalidRedirectUri() {
    assertThatThrownBy(
            () ->
                AuthorizationCodeTokenRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"authorization_code\",\n"
                        + "  \"code\" : \"test-code\",\n"
                        + "  \"redirect_uri\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: redirect_uri: 123");
  }

  @Test
  public void roundTripSerdeMinimal() {
    AuthorizationCodeTokenRequest request =
        ImmutableAuthorizationCodeTokenRequest.builder()
            .code("test-code")
            .redirectUri(URI.create("https://example.com/callback"))
            .build();

    String json = AuthorizationCodeTokenRequestParser.toJson(request, true);
    assertThat(AuthorizationCodeTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"authorization_code\",\n"
                + "  \"code\" : \"test-code\",\n"
                + "  \"redirect_uri\" : \"https://example.com/callback\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithClientCredentials() {
    AuthorizationCodeTokenRequest request =
        ImmutableAuthorizationCodeTokenRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .code("test-code")
            .redirectUri(URI.create("https://example.com/callback"))
            .build();

    String json = AuthorizationCodeTokenRequestParser.toJson(request, true);
    assertThat(AuthorizationCodeTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\",\n"
                + "  \"grant_type\" : \"authorization_code\",\n"
                + "  \"code\" : \"test-code\",\n"
                + "  \"redirect_uri\" : \"https://example.com/callback\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithCodeVerifier() {
    AuthorizationCodeTokenRequest request =
        ImmutableAuthorizationCodeTokenRequest.builder()
            .code("test-code")
            .redirectUri(URI.create("https://example.com/callback"))
            .codeVerifier("test-code-verifier")
            .build();

    String json = AuthorizationCodeTokenRequestParser.toJson(request, true);
    assertThat(AuthorizationCodeTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"authorization_code\",\n"
                + "  \"code\" : \"test-code\",\n"
                + "  \"redirect_uri\" : \"https://example.com/callback\",\n"
                + "  \"code_verifier\" : \"test-code-verifier\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithScopeAndExtraParameters() {
    Map<String, String> extraParams = ImmutableMap.of("custom_param", "custom_value");
    AuthorizationCodeTokenRequest request =
        ImmutableAuthorizationCodeTokenRequest.builder()
            .clientId("test-client-id")
            .code("test-code")
            .redirectUri(URI.create("https://example.com/callback"))
            .codeVerifier("test-code-verifier")
            .scope("read write")
            .extraParameters(extraParams)
            .build();

    String json = AuthorizationCodeTokenRequestParser.toJson(request, true);
    assertThat(AuthorizationCodeTokenRequestParser.fromJson(json)).isEqualTo(request);
    // Verify that the JSON contains all expected fields (order may vary)
    assertThat(json).contains("\"client_id\" : \"test-client-id\"");
    assertThat(json).contains("\"grant_type\" : \"authorization_code\"");
    assertThat(json).contains("\"scope\" : \"read write\"");
    assertThat(json).contains("\"code\" : \"test-code\"");
    assertThat(json).contains("\"redirect_uri\" : \"https://example.com/callback\"");
    assertThat(json).contains("\"code_verifier\" : \"test-code-verifier\"");
    assertThat(json).contains("\"custom_param\" : \"custom_value\"");
  }
}
