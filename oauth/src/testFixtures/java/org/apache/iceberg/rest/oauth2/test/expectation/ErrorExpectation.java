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
package org.apache.iceberg.rest.oauth2.test.expectation;

import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;

@Value.Immutable
@SuppressWarnings("resource")
@OAuth2ImmutableStyle
public abstract class ErrorExpectation extends AbstractExpectation {

  public static final HttpResponse AUTHORIZATION_SERVER_ERROR_RESPONSE =
      HttpResponse.response()
          .withStatusCode(401)
          .withContentType(MediaType.APPLICATION_JSON)
          .withBody(
              JsonBody.json(
                  "{\"error\":\"invalid_request\",\"error_description\":\"Invalid request\"}"));

  @Override
  public void create() {
    clientAndServer()
        .when(
            HttpRequest.request()
                .withPath(testEnvironment().authorizationServerContextPath() + ".*"))
        .respond(AUTHORIZATION_SERVER_ERROR_RESPONSE);
  }
}
