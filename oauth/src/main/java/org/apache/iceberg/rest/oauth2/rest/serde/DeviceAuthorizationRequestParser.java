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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.rest.DeviceAuthorizationRequest;
import org.apache.iceberg.rest.oauth2.rest.ImmutableDeviceAuthorizationRequest;
import org.apache.iceberg.util.JsonUtil;

public class DeviceAuthorizationRequestParser {

  private DeviceAuthorizationRequestParser() {}

  public static String toJson(DeviceAuthorizationRequest request) {
    return toJson(request, false);
  }

  public static String toJson(DeviceAuthorizationRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(DeviceAuthorizationRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid device authorization request: null");
    gen.writeStartObject();
    ClientRequestParser.toJson(request, gen);
    if (request.scope() != null) {
      gen.writeStringField(DeviceAuthorizationRequest.SCOPE, request.scope());
    }

    gen.writeEndObject();
  }

  public static DeviceAuthorizationRequest fromJson(String json) {
    return JsonUtil.parse(json, DeviceAuthorizationRequestParser::fromJson);
  }

  public static DeviceAuthorizationRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse device authorization request from null object");
    DeviceAuthorizationRequest.Builder builder = ImmutableDeviceAuthorizationRequest.builder();
    ClientRequestParser.fromJson(json, builder);
    if (json.hasNonNull(DeviceAuthorizationRequest.SCOPE)) {
      builder.scope(JsonUtil.getString(DeviceAuthorizationRequest.SCOPE, json));
    }

    return builder.build();
  }
}
