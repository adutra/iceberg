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
import org.apache.iceberg.rest.oauth2.rest.ImmutableMetadataDiscoveryResponse;
import org.apache.iceberg.rest.oauth2.rest.MetadataDiscoveryResponse;
import org.junit.jupiter.api.Test;

public class TestMetadataDiscoveryResponseParser {

  @Test
  public void nullResponse() {
    assertThatThrownBy(() -> MetadataDiscoveryResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata discovery response from null object");

    assertThatThrownBy(() -> MetadataDiscoveryResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata discovery response: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> MetadataDiscoveryResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: issuer");

    assertThatThrownBy(
            () -> MetadataDiscoveryResponseParser.fromJson("{\"issuer\": \"https://example.com\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: token_endpoint");
  }

  @Test
  public void invalidIssuer() {
    assertThatThrownBy(
            () ->
                MetadataDiscoveryResponseParser.fromJson(
                    "{\n"
                        + "  \"issuer\" : 123,\n"
                        + "  \"token_endpoint\" : \"https://example.com/token\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: issuer: 123");
  }

  @Test
  public void invalidTokenEndpoint() {
    assertThatThrownBy(
            () ->
                MetadataDiscoveryResponseParser.fromJson(
                    "{\n"
                        + "  \"issuer\" : \"https://example.com\",\n"
                        + "  \"token_endpoint\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: token_endpoint: 123");
  }

  @Test
  public void invalidAuthorizationEndpoint() {
    assertThatThrownBy(
            () ->
                MetadataDiscoveryResponseParser.fromJson(
                    "{\n"
                        + "  \"issuer\" : \"https://example.com\",\n"
                        + "  \"token_endpoint\" : \"https://example.com/token\",\n"
                        + "  \"authorization_endpoint\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: authorization_endpoint: 123");
  }

  @Test
  public void roundTripSerde() {
    MetadataDiscoveryResponse response =
        ImmutableMetadataDiscoveryResponse.builder()
            .issuerUrl(URI.create("https://example.com"))
            .tokenEndpoint(URI.create("https://example.com/token"))
            .authorizationEndpoint(URI.create("https://example.com/authorize"))
            .build();

    String json = MetadataDiscoveryResponseParser.toJson(response, true);
    assertThat(MetadataDiscoveryResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"issuer\" : \"https://example.com\",\n"
                + "  \"authorization_endpoint\" : \"https://example.com/authorize\",\n"
                + "  \"token_endpoint\" : \"https://example.com/token\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithDeviceAuthorizationEndpoint() {
    MetadataDiscoveryResponse response =
        ImmutableMetadataDiscoveryResponse.builder()
            .issuerUrl(URI.create("https://example.com"))
            .tokenEndpoint(URI.create("https://example.com/token"))
            .authorizationEndpoint(URI.create("https://example.com/authorize"))
            .deviceAuthorizationEndpoint(URI.create("https://example.com/device_authorization"))
            .build();

    String json = MetadataDiscoveryResponseParser.toJson(response, true);
    assertThat(MetadataDiscoveryResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"issuer\" : \"https://example.com\",\n"
                + "  \"authorization_endpoint\" : \"https://example.com/authorize\",\n"
                + "  \"token_endpoint\" : \"https://example.com/token\",\n"
                + "  \"device_authorization_endpoint\" : \"https://example.com/device_authorization\"\n"
                + "}");
  }
}
