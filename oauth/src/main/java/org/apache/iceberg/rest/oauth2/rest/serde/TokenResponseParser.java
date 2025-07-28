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
import org.apache.iceberg.rest.oauth2.rest.TokenResponse;
import org.apache.iceberg.util.JsonUtil;

public class TokenResponseParser {

  private static final String ACCESS_TOKEN = "access_token";
  private static final String TOKEN_TYPE = "token_type";
  private static final String EXPIRES_IN = "expires_in";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String REFRESH_EXPIRES_IN = "refresh_expires_in";
  private static final String SCOPE = "scope";

  private TokenResponseParser() {}

  static void writeJsonFields(TokenResponse response, JsonGenerator gen) throws IOException {
    gen.writeStringField(ACCESS_TOKEN, response.accessTokenPayload());
    gen.writeStringField(TOKEN_TYPE, response.tokenType());

    Integer accessTokenExpiresInSeconds = response.accessTokenExpiresInSeconds();
    if (accessTokenExpiresInSeconds != null) {
      gen.writeNumberField(EXPIRES_IN, accessTokenExpiresInSeconds);
    }

    String refreshTokenPayload = response.refreshTokenPayload();
    if (refreshTokenPayload != null) {
      gen.writeStringField(REFRESH_TOKEN, refreshTokenPayload);
    }

    Integer refreshTokenExpiresInSeconds = response.refreshTokenExpiresInSeconds();
    if (refreshTokenExpiresInSeconds != null) {
      gen.writeNumberField(REFRESH_EXPIRES_IN, refreshTokenExpiresInSeconds);
    }

    String scope = response.scope();
    if (scope != null) {
      gen.writeStringField(SCOPE, scope);
    }
  }

  static <T extends TokenResponse, B extends TokenResponse.Builder<T, B>> void readJsonFields(
      JsonNode json, B builder) {

    String accessToken = JsonUtil.getString(ACCESS_TOKEN, json);
    String tokenType = JsonUtil.getString(TOKEN_TYPE, json);

    builder.accessTokenPayload(accessToken).tokenType(tokenType);

    if (json.hasNonNull(EXPIRES_IN)) {
      builder.accessTokenExpiresInSeconds(JsonUtil.getInt(EXPIRES_IN, json));
    }

    if (json.hasNonNull(REFRESH_TOKEN)) {
      builder.refreshTokenPayload(JsonUtil.getString(REFRESH_TOKEN, json));
    }

    if (json.hasNonNull(REFRESH_EXPIRES_IN)) {
      builder.refreshTokenExpiresInSeconds(JsonUtil.getInt(REFRESH_EXPIRES_IN, json));
    }

    if (json.hasNonNull(SCOPE)) {
      builder.scope(JsonUtil.getString(SCOPE, json));
    }
  }
}
