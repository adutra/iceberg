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
import org.apache.iceberg.rest.auth.oauth2.rest.DeviceAuthorizationResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableDeviceAuthorizationResponse;
import org.junit.jupiter.api.Test;

public class TestDeviceAuthorizationResponseParser {

  @Test
  public void nullResponse() {
    assertThatThrownBy(() -> DeviceAuthorizationResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse device authorization response from null object");

    assertThatThrownBy(() -> DeviceAuthorizationResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid device authorization response: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> DeviceAuthorizationResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: device_code");

    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\"device_code\": \"test-device-code\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: user_code");

    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\n"
                        + "  \"device_code\": \"test-device-code\",\n"
                        + "  \"user_code\": \"test-user-code\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: verification_uri");

    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\n"
                        + "  \"device_code\": \"test-device-code\",\n"
                        + "  \"user_code\": \"test-user-code\",\n"
                        + "  \"verification_uri\": \"https://example.com/device\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: expires_in");
  }

  @Test
  public void invalidDeviceCode() {
    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\n"
                        + "  \"device_code\" : 123,\n"
                        + "  \"user_code\" : \"test-user-code\",\n"
                        + "  \"verification_uri\" : \"https://example.com/device\",\n"
                        + "  \"expires_in\" : 1800\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: device_code: 123");
  }

  @Test
  public void invalidUserCode() {
    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\n"
                        + "  \"device_code\" : \"test-device-code\",\n"
                        + "  \"user_code\" : 123,\n"
                        + "  \"verification_uri\" : \"https://example.com/device\",\n"
                        + "  \"expires_in\" : 1800\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: user_code: 123");
  }

  @Test
  public void invalidVerificationUri() {
    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\n"
                        + "  \"device_code\" : \"test-device-code\",\n"
                        + "  \"user_code\" : \"test-user-code\",\n"
                        + "  \"verification_uri\" : 123,\n"
                        + "  \"expires_in\" : 1800\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: verification_uri: 123");
  }

  @Test
  public void invalidExpiresIn() {
    assertThatThrownBy(
            () ->
                DeviceAuthorizationResponseParser.fromJson(
                    "{\n"
                        + "  \"device_code\" : \"test-device-code\",\n"
                        + "  \"user_code\" : \"test-user-code\",\n"
                        + "  \"verification_uri\" : \"https://example.com/device\",\n"
                        + "  \"expires_in\" : \"invalid\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: expires_in: \"invalid\"");
  }

  @Test
  public void roundTripSerde() {
    DeviceAuthorizationResponse response =
        ImmutableDeviceAuthorizationResponse.builder()
            .deviceCode("test-device-code")
            .userCode("test-user-code")
            .verificationUri(URI.create("https://example.com/device"))
            .verificationUriComplete(
                URI.create("https://example.com/device?user_code=test-user-code"))
            .expiresInSeconds(1800)
            .intervalSeconds(5)
            .build();

    String json = DeviceAuthorizationResponseParser.toJson(response, true);
    assertThat(DeviceAuthorizationResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"device_code\" : \"test-device-code\",\n"
                + "  \"user_code\" : \"test-user-code\",\n"
                + "  \"verification_uri\" : \"https://example.com/device\",\n"
                + "  \"verification_uri_complete\" : \"https://example.com/device?user_code=test-user-code\",\n"
                + "  \"expires_in\" : 1800,\n"
                + "  \"interval\" : 5\n"
                + "}");
  }
}
