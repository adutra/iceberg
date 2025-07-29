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
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutablePasswordTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.PasswordTokenRequest;
import org.junit.jupiter.api.Test;

public class TestPasswordTokenRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> PasswordTokenRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse password token request from null object");

    assertThatThrownBy(() -> PasswordTokenRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid password token request: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> PasswordTokenRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: grant_type");

    assertThatThrownBy(() -> PasswordTokenRequestParser.fromJson("{\"grant_type\": \"password\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: username");

    assertThatThrownBy(
            () ->
                PasswordTokenRequestParser.fromJson(
                    "{\"grant_type\": \"password\", \"username\": \"testuser\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: password");
  }

  @Test
  public void invalidGrantType() {
    assertThatThrownBy(
            () -> PasswordTokenRequestParser.fromJson("{\"grant_type\": \"invalid_grant\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid_grant");

    assertThatThrownBy(
            () -> PasswordTokenRequestParser.fromJson("{\"grant_type\": \"client_credentials\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid grant type: client_credentials (expected password)");

    assertThatThrownBy(() -> PasswordTokenRequestParser.fromJson("{\"grant_type\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: grant_type: 123");
  }

  @Test
  public void invalidUsername() {
    assertThatThrownBy(
            () ->
                PasswordTokenRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"password\",\n"
                        + "  \"username\" : 123,\n"
                        + "  \"password\" : \"testpass\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: username: 123");
  }

  @Test
  public void invalidPassword() {
    assertThatThrownBy(
            () ->
                PasswordTokenRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"password\",\n"
                        + "  \"username\" : \"testuser\",\n"
                        + "  \"password\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: password: 123");
  }

  @Test
  public void roundTripSerdeMinimal() {
    PasswordTokenRequest request =
        ImmutablePasswordTokenRequest.builder().username("testuser").password("testpass").build();

    String json = PasswordTokenRequestParser.toJson(request, true);
    assertThat(PasswordTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"password\",\n"
                + "  \"username\" : \"testuser\",\n"
                + "  \"password\" : \"testpass\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithClientCredentials() {
    PasswordTokenRequest request =
        ImmutablePasswordTokenRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .username("testuser")
            .password("testpass")
            .build();

    String json = PasswordTokenRequestParser.toJson(request, true);
    assertThat(PasswordTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\",\n"
                + "  \"grant_type\" : \"password\",\n"
                + "  \"username\" : \"testuser\",\n"
                + "  \"password\" : \"testpass\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithScope() {
    PasswordTokenRequest request =
        ImmutablePasswordTokenRequest.builder()
            .username("testuser")
            .password("testpass")
            .scope("read write")
            .build();

    String json = PasswordTokenRequestParser.toJson(request, true);
    assertThat(PasswordTokenRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"grant_type\" : \"password\",\n"
                + "  \"scope\" : \"read write\",\n"
                + "  \"username\" : \"testuser\",\n"
                + "  \"password\" : \"testpass\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithExtraParameters() {
    Map<String, String> extraParams = ImmutableMap.of("custom_param", "custom_value");
    PasswordTokenRequest request =
        ImmutablePasswordTokenRequest.builder()
            .clientId("test-client-id")
            .username("testuser")
            .password("testpass")
            .scope("read write")
            .extraParameters(extraParams)
            .build();

    String json = PasswordTokenRequestParser.toJson(request, true);
    assertThat(PasswordTokenRequestParser.fromJson(json)).isEqualTo(request);
    // Verify that the JSON contains all expected fields (order may vary)
    assertThat(json).contains("\"client_id\" : \"test-client-id\"");
    assertThat(json).contains("\"grant_type\" : \"password\"");
    assertThat(json).contains("\"scope\" : \"read write\"");
    assertThat(json).contains("\"username\" : \"testuser\"");
    assertThat(json).contains("\"password\" : \"testpass\"");
    assertThat(json).contains("\"custom_param\" : \"custom_value\"");
  }
}
