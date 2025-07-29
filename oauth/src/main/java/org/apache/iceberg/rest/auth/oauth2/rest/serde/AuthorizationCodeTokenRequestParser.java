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
import org.apache.iceberg.rest.auth.oauth2.rest.AuthorizationCodeTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableAuthorizationCodeTokenRequest;
import org.apache.iceberg.util.JsonUtil;

public class AuthorizationCodeTokenRequestParser {

  private static final Set<String> SPECIFIC_FIELDS =
      Set.of(
          AuthorizationCodeTokenRequest.CODE,
          AuthorizationCodeTokenRequest.REDIRECT_URI,
          AuthorizationCodeTokenRequest.CODE_VERIFIER);

  private AuthorizationCodeTokenRequestParser() {}

  public static String toJson(AuthorizationCodeTokenRequest request) {
    return toJson(request, false);
  }

  public static String toJson(AuthorizationCodeTokenRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(AuthorizationCodeTokenRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid authorization code token request: null");

    gen.writeStartObject();

    TokenRequestParser.writeJsonFields(request, gen);

    gen.writeStringField(AuthorizationCodeTokenRequest.CODE, request.code());
    gen.writeStringField(
        AuthorizationCodeTokenRequest.REDIRECT_URI, request.redirectUri().toString());

    if (request.codeVerifier() != null) {
      gen.writeStringField(AuthorizationCodeTokenRequest.CODE_VERIFIER, request.codeVerifier());
    }

    gen.writeEndObject();
  }

  public static AuthorizationCodeTokenRequest fromJson(String json) {
    return JsonUtil.parse(json, AuthorizationCodeTokenRequestParser::fromJson);
  }

  public static AuthorizationCodeTokenRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse authorization code token request from null object");

    AuthorizationCodeTokenRequest.Builder builder =
        ImmutableAuthorizationCodeTokenRequest.builder();

    TokenRequestParser.readJsonFields(json, builder, GrantType.AUTHORIZATION_CODE, SPECIFIC_FIELDS);

    String code = JsonUtil.getString(AuthorizationCodeTokenRequest.CODE, json);
    URI redirectUri =
        URI.create(JsonUtil.getString(AuthorizationCodeTokenRequest.REDIRECT_URI, json));

    builder.code(code).redirectUri(redirectUri);

    if (json.hasNonNull(AuthorizationCodeTokenRequest.CODE_VERIFIER)) {
      builder.codeVerifier(JsonUtil.getString(AuthorizationCodeTokenRequest.CODE_VERIFIER, json));
    }

    return builder.build();
  }
}
