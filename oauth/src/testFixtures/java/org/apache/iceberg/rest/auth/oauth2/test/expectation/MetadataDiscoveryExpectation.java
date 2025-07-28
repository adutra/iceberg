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

import java.net.URI;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.ImmutableMetadataDiscoveryResponse;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

@Value.Immutable
@OAuth2ImmutableStyle
@SuppressWarnings("resource")
public abstract class MetadataDiscoveryExpectation extends AbstractExpectation {

  @Override
  public void create() {
    if (testEnvironment().discoveryEnabled()) {
      URI issuerUrl = testEnvironment().authorizationServerUrl();
      URI discoveryEndpoint = testEnvironment().discoveryEndpoint();
      ImmutableMetadataDiscoveryResponse.Builder builder =
          ImmutableMetadataDiscoveryResponse.builder()
              .issuerUrl(issuerUrl)
              .tokenEndpoint(testEnvironment().tokenEndpoint());

      clientAndServer()
          .when(
              HttpRequest.request()
                  .withMethod("GET")
                  .withPath(discoveryEndpoint.getPath())
                  .withHeader("Accept", "application/json"))
          .respond(HttpResponse.response().withBody(ExpectationUtils.jsonBody(builder.build())));
    }
  }
}
