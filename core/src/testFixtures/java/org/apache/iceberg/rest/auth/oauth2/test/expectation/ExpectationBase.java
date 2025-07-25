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
import com.nimbusds.oauth2.sdk.util.URLUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.immutables.value.Value;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpMessage;
import org.mockserver.model.JsonBody;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;

public abstract class ExpectationBase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    RESTSerializers.registerAll(OBJECT_MAPPER);
  }

  protected static JsonBody jsonBody(RESTResponse body) {
    try {
      return JsonBody.json(OBJECT_MAPPER.writeValueAsString(body));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected static JsonBody jsonBody(Map<String, Object> response) {
    return JsonBody.json(response);
  }

  protected static ParameterBody parameterBody(Map<String, String> request) {
    List<Parameter> parameters =
        request.entrySet().stream()
            .map(entry -> Parameter.param(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    return ParameterBody.params(parameters);
  }

  protected static Map<String, List<String>> decodeBodyParameters(HttpMessage<?, ?> httpMessage) {
    // See https://github.com/mock-server/mockserver/issues/1468
    String body = httpMessage.getBodyAsString();
    return URLUtils.parseParameters(body);
  }

  @Value.Parameter(order = 1)
  protected abstract TestEnvironment testEnvironment();

  public abstract void create();

  @SuppressWarnings("resource")
  protected ClientAndServer mockServer() {
    return testEnvironment().mockServer().orElseThrow();
  }
}
