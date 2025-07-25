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
import org.apache.iceberg.rest.oauth2.rest.ImmutableTokenExchangeRequest;
import org.apache.iceberg.rest.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.apache.iceberg.rest.oauth2.token.TypedToken;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;

@Value.Immutable
@OAuth2ImmutableStyle
public abstract class IcebergRefreshTokenExpectation extends AbstractTokenEndpointExpectation {

  @Override
  @SuppressWarnings("resource")
  public void create() {
    clientAndServer()
        .when(tokenRequest())
        .respond(httpRequest -> tokenResponse(httpRequest, "access_refreshed", null));
  }

  @Override
  protected void addRequestHeaders(HttpRequest request) {
    // accept both Basic and Bearer
    request.withHeader("Authorization", "(Basic|Bearer) .*");
  }

  @Override
  protected PostFormRequest tokenRequestBody() {
    return ImmutableTokenExchangeRequest.builder()
        .subjectToken("access_.*")
        .subjectTokenType(TypedToken.URN_ACCESS_TOKEN)
        .scope(String.format("(%s|%s)", TestConstants.SCOPE1, TestConstants.SCOPE2))
        .putExtraParameter("(extra1|extra2)", "(value1|value2)")
        .build();
  }
}
