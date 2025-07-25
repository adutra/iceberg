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
package org.apache.iceberg.rest.auth.oauth2;

import static org.apache.iceberg.rest.auth.oauth2.OAuth2Config.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.benmanes.caffeine.cache.Cache;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.MapAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

class TestOAuth2Manager {

  /** Tests that instantiate an {@link OAuth2Manager} directly. */
  @Nested
  class UnitTests {

    private final TableIdentifier table = TableIdentifier.of("t1");

    private final HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost:8181"))
            .method(HTTPMethod.GET)
            .path("v1/config")
            .build();

    @Test
    void catalogSessionWithoutInit() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> properties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession session = manager.catalogSession(client, properties)) {
          HTTPRequest actual = session.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void catalogSessionWithInit() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> properties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        try (HTTPClient httpClient = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession session = manager.initSession(httpClient, properties)) {
          HTTPRequest actual = session.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }

        try (HTTPClient httpClient = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession session = manager.catalogSession(httpClient, properties)) {
          HTTPRequest actual = session.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionEmpty() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> properties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        SessionContext context = SessionContext.createEmpty();
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, properties);
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          assertThat(contextualSession).isSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionNotCached() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        // Identical to catalog properties, so should not be cached
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    PREFIX + BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID1.getValue(),
                    PREFIX + BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET1.getValue()),
                Map.of(PREFIX + BasicConfig.SCOPE, TestEnvironment.SCOPE1.toString()));
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          assertThat(contextualSession).isSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionCacheHit() throws IOException {
      try (TestEnvironment env =
              TestEnvironment.builder().grantType(GrantType.TOKEN_EXCHANGE).build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    PREFIX + BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID2.getValue(),
                    PREFIX + BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET2.getValue(),
                    TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN,
                    new TypelessAccessToken("subject").getValue(),
                    TokenExchangeConfig.PREFIX + TokenExchangeConfig.ACTOR_TOKEN,
                    new TypelessAccessToken("actor").getValue()),
                Map.of(
                    PREFIX + BasicConfig.GRANT_TYPE,
                    GrantType.TOKEN_EXCHANGE.getValue(),
                    PREFIX + BasicConfig.SCOPE,
                    TestEnvironment.SCOPE2.toString(),
                    TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES,
                    new Audience("audience").getValue(),
                    TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE,
                    URI.create("urn:iceberg-oauth2-client:test:resource").toString()));
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession contextualSession1 = manager.contextualSession(context, catalogSession);
            AuthSession contextualSession2 = manager.contextualSession(context, catalogSession)) {
          assertThat(contextualSession1).isNotSameAs(catalogSession);
          assertThat(contextualSession2).isNotSameAs(catalogSession);
          assertThat(contextualSession1).isSameAs(contextualSession2);
          HTTPRequest actual = contextualSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionCacheMiss() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        SessionContext context1 =
            new SessionContext(
                "test1",
                "test",
                Map.of(
                    PREFIX + BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID2.getValue(),
                    PREFIX + BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET2.getValue()),
                Map.of(
                    PREFIX + BasicConfig.SCOPE,
                    TestEnvironment.SCOPE2.toString(),
                    PREFIX + BasicConfig.EXTRA_PARAMS + ".extra2",
                    "value2"));
        SessionContext context2 =
            new SessionContext(
                "test2",
                "test",
                Map.of(
                    PREFIX + BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID2.getValue(),
                    PREFIX + BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET2.getValue()),
                Map.of(
                    PREFIX + BasicConfig.SCOPE,
                    TestEnvironment.SCOPE2.toString(),
                    PREFIX + BasicConfig.EXTRA_PARAMS + ".extra2",
                    "value2"));
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession contextualSession1 = manager.contextualSession(context1, catalogSession);
            AuthSession contextualSession2 = manager.contextualSession(context2, catalogSession)) {
          assertThat(contextualSession1).isNotSameAs(catalogSession);
          assertThat(contextualSession2).isNotSameAs(catalogSession);
          assertThat(contextualSession1).isNotSameAs(contextualSession2);
          HTTPRequest actual = contextualSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
          actual = contextualSession2.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionLegacyProperties() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    OAuth2Properties.CREDENTIAL,
                    TestEnvironment.CLIENT_ID2.getValue()
                        + ":"
                        + TestEnvironment.CLIENT_SECRET2.getValue()),
                Map.of(OAuth2Properties.SCOPE, TestEnvironment.SCOPE2.toString()));
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          assertThat(contextualSession).isNotSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionEmpty() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> tableProperties = Map.of();
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          assertThat(tableSession).isSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionNotCached() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        // Identical to catalog properties, so should not be cached
        Map<String, String> tableProperties = Map.copyOf(catalogProperties);
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          assertThat(tableSession).isSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionCacheHit() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> tableProperties =
            Map.of(
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE2.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra2",
                "value2");
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession tableSession1 =
                manager.tableSession(table, tableProperties, catalogSession);
            AuthSession tableSession2 =
                manager.tableSession(table, tableProperties, catalogSession)) {
          assertThat(tableSession1).isNotSameAs(catalogSession);
          assertThat(tableSession2).isNotSameAs(catalogSession);
          assertThat(tableSession1).isSameAs(tableSession2);
          HTTPRequest actual = tableSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionCacheMiss() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra2",
                "value2");
        Map<String, String> tableProperties1 =
            Map.of(
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> tableProperties2 =
            Map.of(
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE2.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra2",
                "value2");
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession tableSession1 =
                manager.tableSession(table, tableProperties1, catalogSession);
            AuthSession tableSession2 =
                manager.tableSession(table, tableProperties2, catalogSession)) {
          assertThat(tableSession1).isNotSameAs(catalogSession);
          assertThat(tableSession2).isNotSameAs(catalogSession);
          assertThat(tableSession1).isNotSameAs(tableSession2);
          HTTPRequest actual = tableSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
          actual = tableSession2.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionLegacyPropertiesVendedToken() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> tableProperties =
            Map.of(
                OAuth2Properties.SCOPE,
                TestEnvironment.SCOPE2.toString(),
                OAuth2Properties.TOKEN,
                "access_vended");
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          assertThat(tableSession).isNotSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_vended"));
        }
      }
    }

    @Test
    void tableSessionLegacyPropertiesVendedTokenExchange() throws IOException {
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .grantType(GrantType.TOKEN_EXCHANGE)
                  .actorToken(Optional.empty())
                  .build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> tableProperties =
            Map.of(
                OAuth2Properties.SCOPE, TestEnvironment.SCOPE2.toString(),
                OAuth2Properties.RESOURCE, env.resource().map(URI::toString).orElseThrow(),
                OAuth2Properties.AUDIENCE, env.audiences().get(0).getValue(),
                OAuth2Properties.ACCESS_TOKEN_TYPE, "access_vended");
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          assertThat(tableSession).isNotSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              // access_initial is the exchanged token
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void standaloneTableSessionCacheMiss() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> standaloneProperties1 =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                CatalogProperties.WAREHOUSE_LOCATION,
                TestEnvironment.WAREHOUSE,
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> standaloneProperties2 =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                CatalogProperties.WAREHOUSE_LOCATION,
                TestEnvironment.WAREHOUSE,
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID2.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET2.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE2.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra2",
                "value2");
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession standaloneSession1 = manager.tableSession(client, standaloneProperties1);
            AuthSession standaloneSession2 = manager.tableSession(client, standaloneProperties2)) {
          assertThat(standaloneSession1).isNotSameAs(standaloneSession2);
          HTTPRequest actual = standaloneSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
          actual = standaloneSession2.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void standaloneTableSessionCacheHit() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> standaloneProperties1 =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                CatalogProperties.WAREHOUSE_LOCATION,
                TestEnvironment.WAREHOUSE,
                PREFIX + BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                PREFIX + BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                PREFIX + BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        // Same OAuth2 config shared by 2 catalog servers => same OAuth2 session
        Map<String, String> standaloneProperties2 =
            ImmutableMap.<String, String>builder()
                .putAll(standaloneProperties1)
                .put(CatalogProperties.URI, "https://other.com")
                .buildKeepingLast();
        try (HTTPClient client = env.newIcebergRestClientBuilder(Map.of()).build();
            AuthSession standaloneSession1 = manager.tableSession(client, standaloneProperties1);
            AuthSession standaloneSession2 = manager.tableSession(client, standaloneProperties2)) {
          assertThat(standaloneSession1).isSameAs(standaloneSession2);
          HTTPRequest actual = standaloneSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }
  }

  /**
   * Tests that instantiate a full {@link RESTCatalog} embedding an {@link OAuth2Manager}. These
   * tests exercise the config and table endpoints.
   */
  @Nested
  class CatalogTests {

    private static final String BY_SESSION_ID_CACHE = "sessionCatalog.authManager.bySessionId";
    private static final String BY_CONFIG_CACHE = "sessionCatalog.authManager.byConfig";

    private final SessionCatalog.SessionContext sessionContext =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            Map.of(
                PREFIX + BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID2.getValue(),
                PREFIX + BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET2.getValue()),
            Map.of(PREFIX + BasicConfig.SCOPE, TestEnvironment.SCOPE2.toString()));

    @CartesianTest
    void testCatalogProperties(
        @EnumLike(
                excludes = {
                  "refresh_token",
                  // cannot test human interaction grants with RESTCatalog
                  "authorization_code",
                  "urn:ietf:params:oauth:grant-type:device_code"
                })
            GrantType grantType,
        @EnumLike ClientAuthenticationMethod authenticationMethod)
        throws IOException {
      assumeTrue(
          !grantType.equals(GrantType.CLIENT_CREDENTIALS)
              || !authenticationMethod.equals(ClientAuthenticationMethod.NONE));
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .grantType(grantType)
                  .clientAuthenticationMethod(authenticationMethod)
                  .build();
          RESTCatalog catalog = env.newCatalog()) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(catalog).extracting(BY_SESSION_ID_CACHE).isNull();
        assertThat(catalog).extracting(BY_CONFIG_CACHE).isNull();
      }
    }

    @Test
    void testCatalogAndContextProperties() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().sessionContext(sessionContext).build();
          RESTCatalog catalog = env.newCatalog()) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(catalog)
            .extracting(BY_SESSION_ID_CACHE, asMap(String.class))
            .satisfies(
                cache -> {
                  assertThat(cache).hasSize(1);
                  String key = cache.keySet().iterator().next();
                  assertThat(key).isEqualTo(sessionContext.sessionId());
                });
        assertThat(catalog).extracting(BY_CONFIG_CACHE).isNull();
      }
    }

    @Test
    void testCatalogAndTableProperties() throws IOException {
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .tableProperties(
                      Map.of(PREFIX + BasicConfig.SCOPE, TestEnvironment.SCOPE2.toString()))
                  .build();
          RESTCatalog catalog = env.newCatalog()) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(catalog).extracting(BY_SESSION_ID_CACHE).isNull();
        assertThat(catalog).extracting(BY_CONFIG_CACHE, asMap(OAuth2Config.class)).hasSize(1);
      }
    }

    @Test
    void testCatalogAndContextAndTableProperties() throws IOException {
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .sessionContext(sessionContext)
                  .tableProperties(
                      Map.of(PREFIX + BasicConfig.SCOPE, TestEnvironment.SCOPE1.toString()))
                  .build();
          RESTCatalog catalog = env.newCatalog()) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(catalog)
            .extracting(BY_SESSION_ID_CACHE, asMap(String.class))
            .satisfies(
                cache -> {
                  assertThat(cache).hasSize(1);
                  String key = cache.keySet().iterator().next();
                  assertThat(key).isEqualTo(sessionContext.sessionId());
                });
        assertThat(catalog).extracting(BY_CONFIG_CACHE, asMap(OAuth2Config.class)).hasSize(1);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K> InstanceOfAssertFactory<Cache, MapAssert<K, OAuth2Session>> asMap(
        Class<K> keyType) {
      return new InstanceOfAssertFactory<Cache, MapAssert<K, OAuth2Session>>(
          Cache.class,
          new Class[] {keyType, OAuth2Session.class},
          actual -> assertThat(actual.asMap()));
    }
  }
}
