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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.rest.ClientRequest;
import org.apache.iceberg.rest.oauth2.rest.TokenRequest;
import org.apache.iceberg.util.JsonUtil;

public abstract class TokenRequestParser {

  private static final Set<String> COMMON_FIELDS =
      Set.of(
          ClientRequest.CLIENT_ID,
          ClientRequest.CLIENT_SECRET,
          TokenRequest.GRANT_TYPE,
          TokenRequest.SCOPE);

  private TokenRequestParser() {}

  public static void toJson(TokenRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid client credentials token request: null");

    ClientRequestParser.toJson(request, gen);

    gen.writeStringField(TokenRequest.GRANT_TYPE, request.grantType().canonicalName());

    if (request.scope() != null) {
      gen.writeStringField(TokenRequest.SCOPE, request.scope());
    }

    // Write extra parameters
    if (!request.extraParameters().isEmpty()) {
      for (Map.Entry<String, String> entry : request.extraParameters().entrySet()) {
        gen.writeStringField(entry.getKey(), entry.getValue());
      }
    }
  }

  public static <T extends TokenRequest, B extends TokenRequest.Builder<T, B>> void fromJson(
      JsonNode json, B builder, GrantType expectedGrantType, Set<String> specificFields) {
    Preconditions.checkArgument(
        null != json, "Cannot parse client credentials token request from null object");

    ClientRequestParser.fromJson(json, builder);

    GrantType grantType =
        GrantType.fromConfigName(JsonUtil.getString(TokenRequest.GRANT_TYPE, json));

    Preconditions.checkArgument(
        grantType.equals(expectedGrantType),
        "Invalid grant type: %s (expected %s)",
        grantType.canonicalName(),
        expectedGrantType.canonicalName());

    if (json.hasNonNull(TokenRequest.SCOPE)) {
      builder.scope(JsonUtil.getString(TokenRequest.SCOPE, json));
    }

    // Parse extra parameters
    Map<String, String> extraParameters = null;
    Iterator<String> fieldNames = json.fieldNames();
    while (fieldNames.hasNext()) {
      String fieldName = fieldNames.next();
      if (!COMMON_FIELDS.contains(fieldName) && !specificFields.contains(fieldName)) {
        if (extraParameters == null) {
          extraParameters = Maps.newHashMap();
        }
        extraParameters.put(fieldName, JsonUtil.getString(fieldName, json));
      }
    }

    if (extraParameters != null) {
      builder.extraParameters(extraParameters);
    }
  }
}
