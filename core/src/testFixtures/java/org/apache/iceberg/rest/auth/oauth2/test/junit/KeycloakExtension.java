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
package org.apache.iceberg.rest.auth.oauth2.test.junit;

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.container.KeycloakContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class KeycloakExtension extends TestEnvironmentExtension
    implements BeforeAllCallback, AfterAllCallback {

  // Client1 is used for client_secret_basic and client_secret_post authentication
  public static final String CLIENT_ID1 = TestEnvironment.CLIENT_ID1.getValue();
  public static final String CLIENT_SECRET1 = TestEnvironment.CLIENT_SECRET1.getValue();
  public static final String CLIENT_AUTH1 =
      ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue();

  // Client2 is used for "none" authentication (public client)
  public static final String CLIENT_ID2 = TestEnvironment.CLIENT_ID2.getValue();
  public static final String CLIENT_AUTH2 = ClientAuthenticationMethod.NONE.getValue();

  // Client3 is used for client_secret_jwt authentication
  public static final String CLIENT_ID3 = "Client3";
  public static final String CLIENT_SECRET3 = Strings.repeat("S3CR3T", 10);
  public static final String CLIENT_AUTH3 = ClientAuthenticationMethod.CLIENT_SECRET_JWT.getValue();

  // Client4 is used for private_key_jwt authentication (RSA)
  public static final String CLIENT_ID4 = "Client4";
  public static final String CLIENT_SECRET4 = "/openssl/rsa_certificate.pem";
  public static final String CLIENT_AUTH4 = ClientAuthenticationMethod.PRIVATE_KEY_JWT.getValue();

  // Client5 is used for private_key_jwt authentication (ECDSA)
  public static final String CLIENT_ID5 = "Client5";
  public static final String CLIENT_SECRET5 = "/openssl/ecdsa_certificate.pem";
  public static final String CLIENT_AUTH5 = ClientAuthenticationMethod.PRIVATE_KEY_JWT.getValue();

  public static final String USERNAME = TestEnvironment.USERNAME;
  public static final String PASSWORD = TestEnvironment.PASSWORD.getValue();

  public static final String SCOPE1 = TestEnvironment.SCOPE1.toString();

  public static final Duration ACCESS_TOKEN_LIFESPAN = Duration.ofSeconds(15);
  public static final Duration REFRESH_TOKEN_LIFESPAN = Duration.ofSeconds(20);

  @Override
  public void beforeAll(ExtensionContext context) {
    KeycloakContainer keycloak =
        new KeycloakContainer()
            .withScope(SCOPE1)
            .withAccessTokenLifespan(ACCESS_TOKEN_LIFESPAN)
            .withRefreshTokenLifespan(REFRESH_TOKEN_LIFESPAN)
            .withUser(USERNAME, PASSWORD)
            .withClient(CLIENT_ID1, CLIENT_SECRET1, CLIENT_AUTH1)
            .withClient(CLIENT_ID2, null, CLIENT_AUTH2)
            .withClient(CLIENT_ID3, CLIENT_SECRET3, CLIENT_AUTH3)
            .withClient(CLIENT_ID4, CLIENT_SECRET4, CLIENT_AUTH4)
            .withClient(CLIENT_ID5, CLIENT_SECRET5, CLIENT_AUTH5);
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
        .serverRootUrl(keycloak.rootUrl())
        .authorizationServerUrl(keycloak.issuerUrl())
        .tokenEndpoint(keycloak.tokenEndpoint())
        .authorizationEndpoint(keycloak.authEndpoint())
        .deviceAuthorizationEndpoint(keycloak.deviceAuthEndpoint())
        .pollInterval(Duration.ofSeconds(1)) // Keycloak's minimum poll interval is 1 second
        .subjectToken(Optional.empty()) // dynamic by default
        .actorToken(Optional.empty()) // dynamic by default
        // must be set to the same values as the main client
        .subjectClientId(TestEnvironment.CLIENT_ID1)
        .subjectClientSecret(TestEnvironment.CLIENT_SECRET1)
        .subjectScope(TestEnvironment.SCOPE1)
        .actorClientId(TestEnvironment.CLIENT_ID1)
        .actorClientSecret(TestEnvironment.CLIENT_SECRET1)
        .actorScope(TestEnvironment.SCOPE1)
        // Unused in Keycloak tests
        .audiences(List.of())
        .resource(Optional.empty());
  }
}
