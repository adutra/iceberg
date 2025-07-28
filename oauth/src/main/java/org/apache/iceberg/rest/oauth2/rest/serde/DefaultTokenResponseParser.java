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
import org.apache.iceberg.rest.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.oauth2.rest.ImmutableDefaultTokenResponse;
import org.apache.iceberg.util.JsonUtil;

public class DefaultTokenResponseParser {

  private DefaultTokenResponseParser() {}

  public static String toJson(DefaultTokenResponse response) {
    return toJson(response, false);
  }

  public static String toJson(DefaultTokenResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(DefaultTokenResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid token response: null");
    gen.writeStartObject();
    TokenResponseParser.writeJsonFields(response, gen);
    gen.writeEndObject();
  }

  public static DefaultTokenResponse fromJson(String json) {
    return JsonUtil.parse(json, DefaultTokenResponseParser::fromJson);
  }

  public static DefaultTokenResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse token response from null object");
    DefaultTokenResponse.Builder builder = ImmutableDefaultTokenResponse.builder();
    TokenResponseParser.readJsonFields(json, builder);
    return builder.build();
  }
}
