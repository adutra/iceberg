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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.immutables.value.Value;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class TokenExchangeExpectation extends TokenEndpointExpectation {

  // Accept both constant tokens (static token tests)
  // and tokens starting with "access_" (dynamic token tests)
  private static final String ACCEPTED_SUBJECT_TOKENS =
      String.format("(%s|%s)", TestEnvironment.SUBJECT_TOKEN, "access_.*");
  private static final String ACCEPTED_ACTOR_TOKENS =
      String.format("(%s|%s)", TestEnvironment.ACTOR_TOKEN, "access_.*");

  @Override
  public void create() {
    mockServer()
        .when(request())
        .respond(httpRequest -> response(httpRequest, "access_initial", "refresh_initial"));
  }

  @Override
  protected ImmutableMap.Builder<String, String> requestBody() {
    ImmutableMap.Builder<String, String> builder =
        super.requestBody()
            .put("grant_type", GrantType.TOKEN_EXCHANGE.toString())
            .put("subject_token", ACCEPTED_SUBJECT_TOKENS)
            .put("subject_token_type", testEnvironment().subjectTokenType().toString())
            .put("requested_token_type", testEnvironment().requestedTokenType().toString())
            .put("scope", ACCEPTED_SCOPES);
    if (testEnvironment().actorToken().isPresent()) {
      builder
          .put("actor_token", ACCEPTED_ACTOR_TOKENS)
          .put("actor_token_type", testEnvironment().actorTokenType().toString());
    }
    for (Audience audience : testEnvironment().audiences()) {
      builder.put("audience", audience.getValue());
    }
    testEnvironment()
        .resource()
        .ifPresent(resource -> builder.put("resource", resource.toString()));
    return builder;
  }
}
