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
import java.net.URI;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.rest.ImmutableMetadataDiscoveryResponse;
import org.apache.iceberg.rest.oauth2.rest.MetadataDiscoveryResponse;
import org.apache.iceberg.util.JsonUtil;

public class MetadataDiscoveryResponseParser {

  private static final String ISSUER = "issuer";
  private static final String AUTHORIZATION_ENDPOINT = "authorization_endpoint";
  private static final String TOKEN_ENDPOINT = "token_endpoint";

  private MetadataDiscoveryResponseParser() {}

  public static String toJson(MetadataDiscoveryResponse response) {
    return toJson(response, false);
  }

  public static String toJson(MetadataDiscoveryResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(MetadataDiscoveryResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid metadata discovery response: null");

    gen.writeStartObject();

    gen.writeStringField(ISSUER, response.issuerUrl().toString());
    gen.writeStringField(AUTHORIZATION_ENDPOINT, response.authorizationEndpoint().toString());
    gen.writeStringField(TOKEN_ENDPOINT, response.tokenEndpoint().toString());

    gen.writeEndObject();
  }

  public static MetadataDiscoveryResponse fromJson(String json) {
    return JsonUtil.parse(json, MetadataDiscoveryResponseParser::fromJson);
  }

  public static MetadataDiscoveryResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse metadata discovery response from null object");

    URI issuerUrl = URI.create(JsonUtil.getString(ISSUER, json));
    URI tokenEndpoint = URI.create(JsonUtil.getString(TOKEN_ENDPOINT, json));
    URI authorizationEndpoint = URI.create(JsonUtil.getString(AUTHORIZATION_ENDPOINT, json));

    ImmutableMetadataDiscoveryResponse.Builder builder =
        ImmutableMetadataDiscoveryResponse.builder()
            .issuerUrl(issuerUrl)
            .tokenEndpoint(tokenEndpoint)
            .authorizationEndpoint(authorizationEndpoint);

    return builder.build();
  }
}
