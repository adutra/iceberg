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
package org.apache.iceberg.rest.oauth2.rest.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.oauth2.rest.ImmutableTokenExchangeRequest;
import org.apache.iceberg.rest.oauth2.rest.TokenExchangeRequest;
import org.junit.jupiter.api.Test;

public class TestTokenExchangeRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> TokenExchangeRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse token exchange request from null object");

    assertThatThrownBy(() -> TokenExchangeRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid token exchange request: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> TokenExchangeRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: grant_type");

    assertThatThrownBy(
            () ->
                TokenExchangeRequestParser.fromJson(
                    "{\n"
                        + "\"grant_type\": \"urn:ietf:params:oauth:grant-type:token-exchange\",\n"
                        + "\"subject_token\": \"test-subject-token\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: subject_token_type");
  }

  @Test
  public void invalidGrantType() {
    assertThatThrownBy(
            () -> TokenExchangeRequestParser.fromJson("{\"grant_type\": \"invalid_grant\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid_grant");

    assertThatThrownBy(
            () -> TokenExchangeRequestParser.fromJson("{\"grant_type\": \"client_credentials\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid grant type: client_credentials (expected urn:ietf:params:oauth:grant-type:token-exchange)");

    assertThatThrownBy(() -> TokenExchangeRequestParser.fromJson("{\"grant_type\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: grant_type: 123");
  }

  @Test
  public void invalidSubjectToken() {
    assertThatThrownBy(
            () ->
                TokenExchangeRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"urn:ietf:params:oauth:grant-type:token-exchange\",\n"
                        + "  \"subject_token\" : 123,\n"
                        + "  \"subject_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: subject_token: 123");
  }

  @Test
  public void invalidSubjectTokenType() {
    assertThatThrownBy(
            () ->
                TokenExchangeRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"urn:ietf:params:oauth:grant-type:token-exchange\",\n"
                        + "  \"subject_token\" : \"test-subject-token\",\n"
                        + "  \"subject_token_type\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: subject_token_type: 123");
  }

  @Test
  public void roundTripSerdeMinimal() {
    TokenExchangeRequest request =
        ImmutableTokenExchangeRequest.builder()
            .subjectToken("test-subject-token")
            .subjectTokenType(URI.create("urn:ietf:params:oauth:token-type:access_token"))
            .build();

    String json = TokenExchangeRequestParser.toJson(request, true);
    assertThat(TokenExchangeRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"urn:ietf:params:oauth:grant-type:token-exchange\",\n"
                + "  \"subject_token\" : \"test-subject-token\",\n"
                + "  \"subject_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithOptionalFields() {
    TokenExchangeRequest request =
        ImmutableTokenExchangeRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .resource(URI.create("https://example.com/resource"))
            .audience("https://example.com/audience")
            .requestedTokenType(URI.create("urn:ietf:params:oauth:token-type:access_token"))
            .subjectToken("test-subject-token")
            .subjectTokenType(URI.create("urn:ietf:params:oauth:token-type:access_token"))
            .actorToken("test-actor-token")
            .actorTokenType(URI.create("urn:ietf:params:oauth:token-type:access_token"))
            .scope("read write")
            .build();

    String json = TokenExchangeRequestParser.toJson(request, true);
    assertThat(TokenExchangeRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\",\n"
                + "  \"grant_type\" : \"urn:ietf:params:oauth:grant-type:token-exchange\",\n"
                + "  \"scope\" : \"read write\",\n"
                + "  \"resource\" : \"https://example.com/resource\",\n"
                + "  \"audience\" : \"https://example.com/audience\",\n"
                + "  \"requested_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\",\n"
                + "  \"subject_token\" : \"test-subject-token\",\n"
                + "  \"subject_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\",\n"
                + "  \"actor_token\" : \"test-actor-token\",\n"
                + "  \"actor_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithExtraParameters() {
    Map<String, String> extraParams = ImmutableMap.of("custom_param", "custom_value");
    TokenExchangeRequest request =
        ImmutableTokenExchangeRequest.builder()
            .subjectToken("test-subject-token")
            .subjectTokenType(URI.create("urn:ietf:params:oauth:token-type:access_token"))
            .extraParameters(extraParams)
            .build();

    String json = TokenExchangeRequestParser.toJson(request, true);
    assertThat(TokenExchangeRequestParser.fromJson(json)).isEqualTo(request);
    // Verify that the JSON contains all expected fields (order may vary)
    assertThat(json)
        .contains("\"grant_type\" : \"urn:ietf:params:oauth:grant-type:token-exchange\"");
    assertThat(json).contains("\"subject_token\" : \"test-subject-token\"");
    assertThat(json)
        .contains("\"subject_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\"");
    assertThat(json).contains("\"custom_param\" : \"custom_value\"");
  }
}
