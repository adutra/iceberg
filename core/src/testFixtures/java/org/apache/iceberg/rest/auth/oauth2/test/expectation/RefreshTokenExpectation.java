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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.immutables.value.Value;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class RefreshTokenExpectation extends TokenEndpointExpectation {

  @Override
  public void create() {
    mockServer()
        .when(request())
        .respond(httpRequest -> response(httpRequest, "access_refreshed", "refresh_refreshed"));
  }

  @Override
  protected ImmutableMap.Builder<String, String> requestBody() {
    if (testEnvironment().refreshGrantType().equals(GrantType.REFRESH_TOKEN)) {
      return super.requestBody()
          .put("grant_type", GrantType.REFRESH_TOKEN.toString())
          .put("refresh_token", "refresh_.*")
          .put("scope", ACCEPTED_SCOPES);
    } else {
      ImmutableMap.Builder<String, String> builder =
          super.requestBody()
              .put("grant_type", GrantType.TOKEN_EXCHANGE.toString())
              .put("subject_token", "access_.*")
              .put("subject_token_type", TokenTypeURI.ACCESS_TOKEN.toString())
              .put("requested_token_type", TokenTypeURI.ACCESS_TOKEN.toString())
              .put("scope", ACCEPTED_SCOPES);
      for (Audience audience : testEnvironment().audiences()) {
        builder.put("audience", audience.getValue());
      }
      testEnvironment()
          .resource()
          .ifPresent(resource -> builder.put("resource", resource.toString()));
      return builder;
    }
  }
}
