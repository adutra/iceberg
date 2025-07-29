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
import org.apache.iceberg.rest.auth.oauth2.rest.DeviceAccessTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableDeviceAccessTokenRequest;
import org.junit.jupiter.api.Test;

public class TestDeviceAccessTokenRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> DeviceAccessTokenRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse device access token request from null object");

    assertThatThrownBy(() -> DeviceAccessTokenRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid device access token request: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> DeviceAccessTokenRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: grant_type");

    assertThatThrownBy(
            () -> DeviceAccessTokenRequestParser.fromJson("{\"grant_type\": \"device_code\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: device_code");
  }

  @Test
  public void invalidGrantType() {
    assertThatThrownBy(
            () -> DeviceAccessTokenRequestParser.fromJson("{\"grant_type\": \"invalid_grant\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown grant type: invalid_grant");

    assertThatThrownBy(
            () ->
                DeviceAccessTokenRequestParser.fromJson("{\"grant_type\": \"client_credentials\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid grant type: client_credentials (expected urn:ietf:params:oauth:grant-type:device_code)");

    assertThatThrownBy(() -> DeviceAccessTokenRequestParser.fromJson("{\"grant_type\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: grant_type: 123");
  }

  @Test
  public void invalidDeviceCode() {
    assertThatThrownBy(
            () ->
                DeviceAccessTokenRequestParser.fromJson(
                    "{\n"
                        + "  \"grant_type\" : \"urn:ietf:params:oauth:grant-type:device_code\",\n"
                        + "  \"device_code\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: device_code: 123");
  }

  @Test
  public void roundTripSerde() {
    Map<String, String> extraParams =
        ImmutableMap.of("custom_param", "custom_value", "another", "value");
    DeviceAccessTokenRequest request =
        ImmutableDeviceAccessTokenRequest.builder()
            .clientId("test-client-id")
            .deviceCode("test-device-code")
            .scope("read write")
            .extraParameters(extraParams)
            .build();

    String json = DeviceAccessTokenRequestParser.toJson(request, true);
    assertThat(DeviceAccessTokenRequestParser.fromJson(json)).isEqualTo(request);
    // Verify that the JSON contains all expected fields (order may vary)
    assertThat(json).contains("\"client_id\" : \"test-client-id\"");
    assertThat(json).contains("\"grant_type\" : \"urn:ietf:params:oauth:grant-type:device_code\"");
    assertThat(json).contains("\"scope\" : \"read write\"");
    assertThat(json).contains("\"device_code\" : \"test-device-code\"");
    assertThat(json).contains("\"another\" : \"value\"");
    assertThat(json).contains("\"custom_param\" : \"custom_value\"");
  }
}
