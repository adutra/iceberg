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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.WWWFormCodec;
import org.apache.iceberg.rest.IcebergCoreTestHooks;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.PostFormRequest;
import org.mockserver.model.HttpMessage;
import org.mockserver.model.JsonBody;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;

public final class ExpectationUtils {

  private ExpectationUtils() {}

  public static JsonBody jsonBody(RESTResponse body) {
    try {
      ObjectMapper objectMapper = IcebergCoreTestHooks.restObjectMapper();
      return JsonBody.json(objectMapper.writeValueAsString(body));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ParameterBody parameterBody(PostFormRequest body) {
    List<Parameter> parameters =
        body.asFormParameters().entrySet().stream()
            .map(entry -> Parameter.param(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    return ParameterBody.params(parameters);
  }

  public static List<NameValuePair> decodeBodyParameters(HttpMessage<?, ?> httpMessage) {
    // See https://github.com/mock-server/mockserver/issues/1468
    String body = httpMessage.getBodyAsString();
    return WWWFormCodec.parse(body, StandardCharsets.UTF_8);
  }

  @Nullable
  public static String findFirstParameterByName(List<NameValuePair> params, String name) {
    return params.stream()
        .filter(pair -> pair.getName().equals(name))
        .map(NameValuePair::getValue)
        .findFirst()
        .orElse(null);
  }
}
