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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.rest.DeviceAuthorizationResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableDeviceAuthorizationResponse;
import org.apache.iceberg.util.JsonUtil;

public class DeviceAuthorizationResponseParser {

  private static final String DEVICE_CODE = "device_code";
  private static final String USER_CODE = "user_code";
  private static final String VERIFICATION_URI = "verification_uri";
  private static final String VERIFICATION_URI_COMPLETE = "verification_uri_complete";
  private static final String EXPIRES_IN = "expires_in";
  private static final String INTERVAL = "interval";

  private DeviceAuthorizationResponseParser() {}

  public static String toJson(DeviceAuthorizationResponse response) {
    return toJson(response, false);
  }

  public static String toJson(DeviceAuthorizationResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(DeviceAuthorizationResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid device authorization response: null");

    gen.writeStartObject();

    gen.writeStringField(DEVICE_CODE, response.deviceCode());
    gen.writeStringField(USER_CODE, response.userCode());
    gen.writeStringField(VERIFICATION_URI, response.verificationUri().toString());

    URI verificationUriComplete = response.verificationUriComplete();
    if (verificationUriComplete != null) {
      gen.writeStringField(VERIFICATION_URI_COMPLETE, verificationUriComplete.toString());
    }

    gen.writeNumberField(EXPIRES_IN, response.expiresInSeconds());

    Integer intervalSeconds = response.intervalSeconds();
    if (intervalSeconds != null) {
      gen.writeNumberField(INTERVAL, intervalSeconds);
    }

    gen.writeEndObject();
  }

  public static DeviceAuthorizationResponse fromJson(String json) {
    return JsonUtil.parse(json, DeviceAuthorizationResponseParser::fromJson);
  }

  public static DeviceAuthorizationResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse device authorization response from null object");

    String deviceCode = JsonUtil.getString(DEVICE_CODE, json);
    String userCode = JsonUtil.getString(USER_CODE, json);
    URI verificationUri = URI.create(JsonUtil.getString(VERIFICATION_URI, json));
    int expiresIn = JsonUtil.getInt(EXPIRES_IN, json);

    ImmutableDeviceAuthorizationResponse.Builder builder =
        ImmutableDeviceAuthorizationResponse.builder()
            .deviceCode(deviceCode)
            .userCode(userCode)
            .verificationUri(verificationUri)
            .expiresInSeconds(expiresIn);

    if (json.hasNonNull(VERIFICATION_URI_COMPLETE)) {
      builder.verificationUriComplete(
          URI.create(JsonUtil.getString(VERIFICATION_URI_COMPLETE, json)));
    }

    if (json.hasNonNull(INTERVAL)) {
      builder.intervalSeconds(JsonUtil.getInt(INTERVAL, json));
    }

    return builder.build();
  }
}
