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
import org.apache.iceberg.rest.auth.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableClientCredentialsTokenRequest;
import org.apache.iceberg.util.JsonUtil;

public class ClientCredentialsTokenRequestParser {

  private ClientCredentialsTokenRequestParser() {}

  public static String toJson(ClientCredentialsTokenRequest request) {
    return toJson(request, false);
  }

  public static String toJson(ClientCredentialsTokenRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(ClientCredentialsTokenRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid client credentials token request: null");
    gen.writeStartObject();
    TokenRequestParser.writeJsonFields(request, gen);
    gen.writeEndObject();
  }

  public static ClientCredentialsTokenRequest fromJson(String json) {
    return JsonUtil.parse(json, ClientCredentialsTokenRequestParser::fromJson);
  }

  public static ClientCredentialsTokenRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse client credentials token request from null object");
    ClientCredentialsTokenRequest.Builder builder =
        ImmutableClientCredentialsTokenRequest.builder();
    TokenRequestParser.readJsonFields(json, builder, GrantType.CLIENT_CREDENTIALS, Set.of());
    return builder.build();
  }
}
