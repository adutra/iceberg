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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.oauth2.rest.ImmutableRefreshTokenRequest;
import org.apache.iceberg.rest.oauth2.rest.RefreshTokenRequest;
import org.junit.jupiter.api.Test;

public class TestRefreshTokenRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> RefreshTokenRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse refresh token request from null object");

    assertThatThrownBy(() -> RefreshTokenRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid refresh token request: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> RefreshTokenRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: grant_type");

    assertThatThrownBy(
            () -> RefreshTokenRequestParser.fromJson("{\"grant_type\": \"refresh_token\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: refresh_token");
  }

  @Test
  public void invalidGrantType() {
    assertThatThrownBy(
            () -> RefreshTokenRequestParser.fromJson("{\"grant_type\": \"invalid_grant\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid_grant");

    assertThatThrownBy(
            () -> RefreshTokenRequestParser.fromJson("{\"grant_type\": \"client_credentials\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid grant type: client_credentials (expected refresh_token)");

    assertThatThrownBy(() -> RefreshTokenRequestParser.fromJson("{\"grant_type\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: grant_type: 123");
  }

  @Test
  public void invalidRefreshToken() {
    assertThatThrownBy(
            () ->
                RefreshTokenRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"refresh_token\",\n"
                        + "  \"refresh_token\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: refresh_token: 123");
  }

  @Test
  public void roundTripSerdeMinimal() {
    RefreshTokenRequest request =
        ImmutableRefreshTokenRequest.builder().refreshToken("test-refresh-token").build();

    String json = RefreshTokenRequestParser.toJson(request, true);
    assertThat(RefreshTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"refresh_token\",\n"
                + "  \"refresh_token\" : \"test-refresh-token\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithClientCredentials() {
    RefreshTokenRequest request =
        ImmutableRefreshTokenRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .refreshToken("test-refresh-token")
            .build();

    String json = RefreshTokenRequestParser.toJson(request, true);
    assertThat(RefreshTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\",\n"
                + "  \"grant_type\" : \"refresh_token\",\n"
                + "  \"refresh_token\" : \"test-refresh-token\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithScope() {
    RefreshTokenRequest request =
        ImmutableRefreshTokenRequest.builder()
            .refreshToken("test-refresh-token")
            .scope("read write")
            .build();

    String json = RefreshTokenRequestParser.toJson(request, true);
    assertThat(RefreshTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"refresh_token\",\n"
                + "  \"scope\" : \"read write\",\n"
                + "  \"refresh_token\" : \"test-refresh-token\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithExtraParameters() {
    Map<String, String> extraParams =
        ImmutableMap.of("custom_param", "custom_value", "another", "value");
    RefreshTokenRequest request =
        ImmutableRefreshTokenRequest.builder()
            .clientId("test-client-id")
            .refreshToken("test-refresh-token")
            .scope("read write")
            .extraParameters(extraParams)
            .build();

    String json = RefreshTokenRequestParser.toJson(request, true);
    assertThat(RefreshTokenRequestParser.fromJson(json)).isEqualTo(request);
    // Verify that the JSON contains all expected fields (order may vary)
    assertThat(json).contains("\"client_id\" : \"test-client-id\"");
    assertThat(json).contains("\"grant_type\" : \"refresh_token\"");
    assertThat(json).contains("\"scope\" : \"read write\"");
    assertThat(json).contains("\"refresh_token\" : \"test-refresh-token\"");
    assertThat(json).contains("\"another\" : \"value\"");
    assertThat(json).contains("\"custom_param\" : \"custom_value\"");
  }
}
