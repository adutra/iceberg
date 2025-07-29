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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.rest.DeviceAccessTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableDeviceAccessTokenRequest;
import org.apache.iceberg.util.JsonUtil;

public class DeviceAccessTokenRequestParser {

  private static final Set<String> SPECIFIC_FIELDS = Set.of(DeviceAccessTokenRequest.DEVICE_CODE);

  private DeviceAccessTokenRequestParser() {}

  public static String toJson(DeviceAccessTokenRequest request) {
    return toJson(request, false);
  }

  public static String toJson(DeviceAccessTokenRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(DeviceAccessTokenRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid device access token request: null");
    gen.writeStartObject();
    TokenRequestParser.writeJsonFields(request, gen);
    gen.writeStringField(DeviceAccessTokenRequest.DEVICE_CODE, request.deviceCode());
    gen.writeEndObject();
  }

  public static DeviceAccessTokenRequest fromJson(String json) {
    return JsonUtil.parse(json, DeviceAccessTokenRequestParser::fromJson);
  }

  public static DeviceAccessTokenRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse device access token request from null object");
    DeviceAccessTokenRequest.Builder builder = ImmutableDeviceAccessTokenRequest.builder();
    TokenRequestParser.readJsonFields(json, builder, GrantType.DEVICE_CODE, SPECIFIC_FIELDS);
    builder.deviceCode(JsonUtil.getString(DeviceAccessTokenRequest.DEVICE_CODE, json));
    return builder.build();
  }
}
