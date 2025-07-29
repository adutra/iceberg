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
package org.apache.iceberg.rest.auth.oauth2.test.container;

import java.time.Clock;
import java.util.List;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironmentExtension;
import org.apache.iceberg.rest.auth.oauth2.token.TypedToken;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class KeycloakTestEnvironment extends TestEnvironmentExtension
    implements BeforeAllCallback, AfterAllCallback {

  @Override
  public void beforeAll(ExtensionContext context) {
    KeycloakContainer keycloak = new KeycloakContainer();
    keycloak.start();
    context
        .getStore(ExtensionContext.Namespace.GLOBAL)
        .put(KeycloakContainer.class.getName(), keycloak);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    KeycloakContainer keycloak =
        context
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .remove(KeycloakContainer.class.getName(), KeycloakContainer.class);
    if (keycloak != null) {
      keycloak.close();
    }
  }

  @Override
  protected ImmutableTestEnvironment.Builder newTestEnvironmentBuilder(ExtensionContext context) {
    KeycloakContainer keycloak =
        context
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .get(KeycloakContainer.class.getName(), KeycloakContainer.class);
    return TestEnvironment.builder()
        .unitTest(false)
        .clock(Clock.systemUTC())
        .serverRootUrl(keycloak.rootUrl())
        .authorizationServerUrl(keycloak.issuerUrl())
        .tokenEndpoint(keycloak.tokenEndpoint())
        .authorizationEndpoint(keycloak.authEndpoint())
        .deviceAuthorizationEndpoint(keycloak.deviceAuthEndpoint())
        .accessTokenLifespan(keycloak.accessTokenLifespan())
        .subjectToken(null) // dynamic by default
        .actorToken(null) // dynamic by default
        .subjectTokenType(TypedToken.URN_ACCESS_TOKEN)
        .actorTokenType(TypedToken.URN_ACCESS_TOKEN)
        // must be set to the same values as the main client
        .subjectClientId(TestConstants.CLIENT_ID1)
        .subjectClientSecret(TestConstants.CLIENT_SECRET1)
        .subjectScopes(List.of(TestConstants.SCOPE1))
        .actorClientId(TestConstants.CLIENT_ID1)
        .actorClientSecret(TestConstants.CLIENT_SECRET1)
        .actorScopes(List.of(TestConstants.SCOPE1))
        // Unused
        .audience(null)
        .resource(null);
  }
}
