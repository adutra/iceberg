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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.rest.ImmutableRefreshTokenRequest;
import org.apache.iceberg.rest.oauth2.rest.RefreshTokenRequest;
import org.apache.iceberg.util.JsonUtil;

public class RefreshTokenRequestParser {

  private static final Set<String> SPECIFIC_FIELDS = Set.of(RefreshTokenRequest.REFRESH_TOKEN);

  private RefreshTokenRequestParser() {}

  public static String toJson(RefreshTokenRequest request) {
    return toJson(request, false);
  }

  public static String toJson(RefreshTokenRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(RefreshTokenRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid refresh token request: null");
    gen.writeStartObject();
    TokenRequestParser.writeJsonFields(request, gen);
    gen.writeStringField(RefreshTokenRequest.REFRESH_TOKEN, request.refreshToken());
    gen.writeEndObject();
  }

  public static RefreshTokenRequest fromJson(String json) {
    return JsonUtil.parse(json, RefreshTokenRequestParser::fromJson);
  }

  public static RefreshTokenRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse refresh token request from null object");
    RefreshTokenRequest.Builder builder = ImmutableRefreshTokenRequest.builder();
    TokenRequestParser.readJsonFields(json, builder, GrantType.REFRESH_TOKEN, SPECIFIC_FIELDS);
    builder.refreshToken(JsonUtil.getString(RefreshTokenRequest.REFRESH_TOKEN, json));
    return builder.build();
  }
}
