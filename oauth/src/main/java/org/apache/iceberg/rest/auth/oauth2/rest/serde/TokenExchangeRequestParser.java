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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableTokenExchangeRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.util.JsonUtil;

public class TokenExchangeRequestParser {

  private static final Set<String> SPECIFIC_FIELDS =
      Set.of(
          TokenExchangeRequest.RESOURCE,
          TokenExchangeRequest.AUDIENCE,
          TokenExchangeRequest.REQUESTED_TOKEN_TYPE,
          TokenExchangeRequest.SUBJECT_TOKEN,
          TokenExchangeRequest.SUBJECT_TOKEN_TYPE,
          TokenExchangeRequest.ACTOR_TOKEN,
          TokenExchangeRequest.ACTOR_TOKEN_TYPE);

  private TokenExchangeRequestParser() {}

  public static String toJson(TokenExchangeRequest request) {
    return toJson(request, false);
  }

  public static String toJson(TokenExchangeRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(TokenExchangeRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid token exchange request: null");

    gen.writeStartObject();

    TokenRequestParser.writeJsonFields(request, gen);

    URI resource = request.resource();
    if (resource != null) {
      gen.writeStringField(TokenExchangeRequest.RESOURCE, resource.toString());
    }

    String audience = request.audience();
    if (audience != null) {
      gen.writeStringField(TokenExchangeRequest.AUDIENCE, audience);
    }

    URI requestedTokenType = request.requestedTokenType();
    if (requestedTokenType != null) {
      gen.writeStringField(
          TokenExchangeRequest.REQUESTED_TOKEN_TYPE, requestedTokenType.toString());
    }

    gen.writeStringField(TokenExchangeRequest.SUBJECT_TOKEN, request.subjectToken());
    gen.writeStringField(
        TokenExchangeRequest.SUBJECT_TOKEN_TYPE, request.subjectTokenType().toString());

    String actorToken = request.actorToken();
    if (actorToken != null) {
      gen.writeStringField(TokenExchangeRequest.ACTOR_TOKEN, actorToken);
    }

    URI actorTokenType = request.actorTokenType();
    if (actorTokenType != null) {
      gen.writeStringField(TokenExchangeRequest.ACTOR_TOKEN_TYPE, actorTokenType.toString());
    }

    gen.writeEndObject();
  }

  public static TokenExchangeRequest fromJson(String json) {
    return JsonUtil.parse(json, TokenExchangeRequestParser::fromJson);
  }

  public static TokenExchangeRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse token exchange request from null object");

    TokenExchangeRequest.Builder builder = ImmutableTokenExchangeRequest.builder();

    TokenRequestParser.readJsonFields(json, builder, GrantType.TOKEN_EXCHANGE, SPECIFIC_FIELDS);

    builder
        .subjectToken(JsonUtil.getString(TokenExchangeRequest.SUBJECT_TOKEN, json))
        .subjectTokenType(
            URI.create(JsonUtil.getString(TokenExchangeRequest.SUBJECT_TOKEN_TYPE, json)));

    if (json.hasNonNull(TokenExchangeRequest.RESOURCE)) {
      builder.resource(URI.create(JsonUtil.getString(TokenExchangeRequest.RESOURCE, json)));
    }

    if (json.hasNonNull(TokenExchangeRequest.AUDIENCE)) {
      builder.audience(JsonUtil.getString(TokenExchangeRequest.AUDIENCE, json));
    }

    if (json.hasNonNull(TokenExchangeRequest.REQUESTED_TOKEN_TYPE)) {
      builder.requestedTokenType(
          URI.create(JsonUtil.getString(TokenExchangeRequest.REQUESTED_TOKEN_TYPE, json)));
    }

    if (json.hasNonNull(TokenExchangeRequest.ACTOR_TOKEN)) {
      builder.actorToken(JsonUtil.getString(TokenExchangeRequest.ACTOR_TOKEN, json));
    }

    if (json.hasNonNull(TokenExchangeRequest.ACTOR_TOKEN_TYPE)) {
      builder.actorTokenType(
          URI.create(JsonUtil.getString(TokenExchangeRequest.ACTOR_TOKEN_TYPE, json)));
    }

    return builder.build();
  }
}
