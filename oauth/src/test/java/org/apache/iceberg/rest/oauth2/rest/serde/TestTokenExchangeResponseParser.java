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
import org.apache.iceberg.rest.oauth2.rest.ImmutableTokenExchangeResponse;
import org.apache.iceberg.rest.oauth2.rest.TokenExchangeResponse;
import org.junit.jupiter.api.Test;

public class TestTokenExchangeResponseParser {

  @Test
  public void nullResponse() {
    assertThatThrownBy(() -> TokenExchangeResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse token response from null object");

    assertThatThrownBy(() -> TokenExchangeResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid token response: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> TokenExchangeResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: access_token");

    assertThatThrownBy(
            () -> TokenExchangeResponseParser.fromJson("{\"access_token\": \"test-access-token\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: token_type");
  }

  @Test
  public void invalidAccessToken() {
    assertThatThrownBy(
            () ->
                TokenExchangeResponseParser.fromJson(
                    "{\n"
                        + "  \"access_token\" : 123,\n"
                        + "  \"token_type\" : \"Bearer\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: access_token: 123");
  }

  @Test
  public void invalidTokenType() {
    assertThatThrownBy(
            () ->
                TokenExchangeResponseParser.fromJson(
                    "{\n"
                        + "  \"access_token\" : \"test-access-token\",\n"
                        + "  \"token_type\" : 123\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: token_type: 123");
  }

  @Test
  public void invalidExpiresIn() {
    assertThatThrownBy(
            () ->
                TokenExchangeResponseParser.fromJson(
                    "{\n"
                        + "  \"access_token\" : \"test-access-token\",\n"
                        + "  \"token_type\" : \"Bearer\",\n"
                        + "  \"expires_in\" : \"invalid\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: expires_in: \"invalid\"");
  }

  @Test
  public void roundTripSerdeMinimal() {
    TokenExchangeResponse response =
        ImmutableTokenExchangeResponse.builder()
            .accessTokenPayload("test-access-token")
            .tokenType("Bearer")
            .build();

    String json = TokenExchangeResponseParser.toJson(response, true);
    assertThat(TokenExchangeResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"access_token\" : \"test-access-token\",\n"
                + "  \"token_type\" : \"Bearer\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithExpiresIn() {
    TokenExchangeResponse response =
        ImmutableTokenExchangeResponse.builder()
            .accessTokenPayload("test-access-token")
            .tokenType("Bearer")
            .accessTokenExpiresInSeconds(3600)
            .build();

    String json = TokenExchangeResponseParser.toJson(response, true);
    assertThat(TokenExchangeResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"access_token\" : \"test-access-token\",\n"
                + "  \"token_type\" : \"Bearer\",\n"
                + "  \"expires_in\" : 3600\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithRefreshToken() {
    TokenExchangeResponse response =
        ImmutableTokenExchangeResponse.builder()
            .accessTokenPayload("test-access-token")
            .tokenType("Bearer")
            .refreshTokenPayload("test-refresh-token")
            .refreshTokenExpiresInSeconds(7200)
            .build();

    String json = TokenExchangeResponseParser.toJson(response, true);
    assertThat(TokenExchangeResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"access_token\" : \"test-access-token\",\n"
                + "  \"token_type\" : \"Bearer\",\n"
                + "  \"refresh_token\" : \"test-refresh-token\",\n"
                + "  \"refresh_expires_in\" : 7200\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithAllFields() {
    TokenExchangeResponse response =
        ImmutableTokenExchangeResponse.builder()
            .accessTokenPayload("test-access-token")
            .tokenType("Bearer")
            .accessTokenExpiresInSeconds(3600)
            .refreshTokenPayload("test-refresh-token")
            .refreshTokenExpiresInSeconds(7200)
            .scope("read write")
            .issuedTokenType(URI.create("urn:ietf:params:oauth:token-type:access_token"))
            .build();

    String json = TokenExchangeResponseParser.toJson(response, true);
    assertThat(TokenExchangeResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"access_token\" : \"test-access-token\",\n"
                + "  \"token_type\" : \"Bearer\",\n"
                + "  \"expires_in\" : 3600,\n"
                + "  \"refresh_token\" : \"test-refresh-token\",\n"
                + "  \"refresh_expires_in\" : 7200,\n"
                + "  \"scope\" : \"read write\",\n"
                + "  \"issued_token_type\" : \"urn:ietf:params:oauth:token-type:access_token\"\n"
                + "}");
  }
}
