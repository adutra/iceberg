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
import org.apache.iceberg.rest.oauth2.rest.DeviceAuthorizationRequest;
import org.apache.iceberg.rest.oauth2.rest.ImmutableDeviceAuthorizationRequest;
import org.junit.jupiter.api.Test;

public class TestDeviceAuthorizationRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> DeviceAuthorizationRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse device authorization request from null object");

    assertThatThrownBy(() -> DeviceAuthorizationRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid device authorization request: null");
  }

  @Test
  public void invalidClientId() {
    assertThatThrownBy(() -> DeviceAuthorizationRequestParser.fromJson("{\"client_id\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: client_id: 123");
  }

  @Test
  public void invalidClientSecret() {
    assertThatThrownBy(() -> DeviceAuthorizationRequestParser.fromJson("{\"client_secret\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: client_secret: 123");
  }

  @Test
  public void invalidScope() {
    assertThatThrownBy(() -> DeviceAuthorizationRequestParser.fromJson("{\"scope\": 123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: scope: 123");
  }

  @Test
  public void roundTripSerdeMinimal() {
    DeviceAuthorizationRequest request = ImmutableDeviceAuthorizationRequest.builder().build();

    String json = DeviceAuthorizationRequestParser.toJson(request, true);
    assertThat(DeviceAuthorizationRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json).isEqualTo("{ }");
  }

  @Test
  public void roundTripSerdeWithClientCredentials() {
    DeviceAuthorizationRequest request =
        ImmutableDeviceAuthorizationRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .build();

    String json = DeviceAuthorizationRequestParser.toJson(request, true);
    assertThat(DeviceAuthorizationRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithScope() {
    DeviceAuthorizationRequest request =
        ImmutableDeviceAuthorizationRequest.builder().scope("read write").build();

    String json = DeviceAuthorizationRequestParser.toJson(request, true);
    assertThat(DeviceAuthorizationRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json).isEqualTo("{\n" + "  \"scope\" : \"read write\"\n" + "}");
  }

  @Test
  public void roundTripSerdeWithAllFields() {
    DeviceAuthorizationRequest request =
        ImmutableDeviceAuthorizationRequest.builder()
            .clientId("test-client-id")
            .clientSecret("test-client-secret")
            .scope("read write")
            .build();

    String json = DeviceAuthorizationRequestParser.toJson(request, true);
    assertThat(DeviceAuthorizationRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"client_id\" : \"test-client-id\",\n"
                + "  \"client_secret\" : \"test-client-secret\",\n"
                + "  \"scope\" : \"read write\"\n"
                + "}");
  }

  @Test
  public void roundTripSerdeEmptyJson() {
    DeviceAuthorizationRequest request = DeviceAuthorizationRequestParser.fromJson("{}");
    assertThat(request.clientId()).isNull();
    assertThat(request.clientSecret()).isNull();
    assertThat(request.scope()).isNull();

    String json = DeviceAuthorizationRequestParser.toJson(request, true);
    assertThat(DeviceAuthorizationRequestParser.fromJson(json)).isEqualTo(request);
  }
}
