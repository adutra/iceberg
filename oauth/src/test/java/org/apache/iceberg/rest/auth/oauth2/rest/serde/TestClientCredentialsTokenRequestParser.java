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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableClientCredentialsTokenRequest;
import org.junit.jupiter.api.Test;

public class TestClientCredentialsTokenRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> ClientCredentialsTokenRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse client credentials token request from null object");

    assertThatThrownBy(() -> ClientCredentialsTokenRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid client credentials token request: null");
  }

  @Test
  public void missingGrantType() {
    assertThatThrownBy(() -> ClientCredentialsTokenRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: grant_type");
  }

  @Test
  public void invalidGrantType() {
    assertThatThrownBy(
            () ->
                ClientCredentialsTokenRequestParser.fromJson("{\"grant_type\": \"invalid_grant\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid_grant");

    assertThatThrownBy(
            () ->
                ClientCredentialsTokenRequestParser.fromJson("{\"grant_type\": \"refresh_token\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid grant type: refresh_token (expected client_credentials)");

    assertThatThrownBy(() -> ClientCredentialsTokenRequestParser.fromJson("{\"grant_type\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: grant_type: 123");
  }

  @Test
  public void invalidClientId() {
    assertThatThrownBy(
            () ->
                ClientCredentialsTokenRequestParser.fromJson(
                    "{\"grant_type\": \"client_credentials\", \"client_id\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: client_id: 123");
  }

  @Test
  public void invalidClientSecret() {
    assertThatThrownBy(
            () ->
                ClientCredentialsTokenRequestParser.fromJson(
                    "{\"grant_type\": \"client_credentials\", \"client_secret\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: client_secret: 123");
  }

  @Test
  public void invalidScope() {
    assertThatThrownBy(
            () ->
                ClientCredentialsTokenRequestParser.fromJson(
                    "{\"grant_type\": \"client_credentials\", \"scope\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: scope: 123");
  }

  @Test
  public void roundTripSerdeMinimal() {
    ClientCredentialsTokenRequest request =
        ImmutableClientCredentialsTokenRequest.builder().build();

    String json = ClientCredentialsTokenRequestParser.toJson(request, true);
    assertThat(ClientCredentialsTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json).isEqualTo("{\n" + "  \"grant_type\" : \"client_credentials\"\n" + "}");
  }

  @Test
  public void roundTripSerdeWithClientCredentials() {
    ClientCredentialsTokenRequest request =
        ImmutableClientCredentialsTokenRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .build();

    String json = ClientCredentialsTokenRequestParser.toJson(request, true);
    assertThat(ClientCredentialsTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\",\n"
                + "  \"grant_type\" : \"client_credentials\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithScope() {
    ClientCredentialsTokenRequest request =
        ImmutableClientCredentialsTokenRequest.builder()
            .clientId("test-client-id")
            .scope("read write")
            .build();

    String json = ClientCredentialsTokenRequestParser.toJson(request, true);
    assertThat(ClientCredentialsTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"grant_type\" : \"client_credentials\",\n"
                + "  \"scope\" : \"read write\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithExtraParameters() {
    Map<String, String> extraParams =
        ImmutableMap.of("custom_param", "custom_value", "another", "value");
    ClientCredentialsTokenRequest request =
        ImmutableClientCredentialsTokenRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .scope("read write")
            .extraParameters(extraParams)
            .build();

    String json = ClientCredentialsTokenRequestParser.toJson(request, true);
    assertThat(ClientCredentialsTokenRequestParser.fromJson(json)).isEqualTo(request);
    // Verify that the JSON contains all expected fields (order may vary)
    assertThat(json).contains("\"client_id\" : \"test-client-id\"");
    assertThat(json).contains("\"client_secret\" : \"test-client-secret\"");
    assertThat(json).contains("\"grant_type\" : \"client_credentials\"");
    assertThat(json).contains("\"scope\" : \"read write\"");
    assertThat(json).contains("\"another\" : \"value\"");
    assertThat(json).contains("\"custom_param\" : \"custom_value\"");
  }
}
