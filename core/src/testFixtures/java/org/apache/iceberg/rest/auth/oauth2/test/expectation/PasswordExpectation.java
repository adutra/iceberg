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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.immutables.value.Value;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class PasswordExpectation extends TokenEndpointExpectation {

  @Override
  public void create() {
    mockServer()
        .when(request())
        .respond(httpRequest -> response(httpRequest, "access_initial", "refresh_initial"));
  }

  @Override
  protected ImmutableMap.Builder<String, String> requestBody() {
    return super.requestBody()
        .put("grant_type", "password")
        .put("username", TestEnvironment.USERNAME)
        .put("password", TestEnvironment.PASSWORD.getValue())
        .put("scope", ACCEPTED_SCOPES);
  }
}
