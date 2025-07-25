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

import com.nimbusds.oauth2.sdk.Scope;
import java.time.Clock;
import java.time.Duration;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.container.PolarisContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class PolarisExtension extends TestEnvironmentExtension
    implements BeforeAllCallback, AfterAllCallback {

  public static final String CLIENT_ID = TestEnvironment.CLIENT_ID1.getValue();
  public static final String CLIENT_SECRET = TestEnvironment.CLIENT_SECRET1.getValue();

  public static final Duration ACCESS_TOKEN_LIFESPAN = Duration.ofSeconds(15);

  @Override
  public void beforeAll(ExtensionContext context) {
    PolarisContainer polaris =
        new PolarisContainer()
            .withClient(CLIENT_ID, CLIENT_SECRET)
            .withAccessTokenLifespan(ACCESS_TOKEN_LIFESPAN);
    polaris.start();
    context
        .getStore(ExtensionContext.Namespace.GLOBAL)
        .put(PolarisContainer.class.getName(), polaris);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    PolarisContainer polaris =
        context
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .remove(PolarisContainer.class.getName(), PolarisContainer.class);
    if (polaris != null) {
      polaris.close();
    }
  }

  @Override
  protected ImmutableTestEnvironment.Builder newTestEnvironmentBuilder(ExtensionContext context) {
    PolarisContainer polaris =
        context
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .get(PolarisContainer.class.getName(), PolarisContainer.class);
    return TestEnvironment.builder()
        .unitTest(false)
        .discoveryEnabled(false)
        .serverRootUrl(polaris.baseUri())
        .tokenEndpoint(polaris.tokenEndpoint())
        .catalogServerContextPath("/api/catalog/")
        .scope(new Scope("PRINCIPAL_ROLE:ALL"))
        .clock(Clock.systemUTC())
        .accessTokenLifespan(polaris.accessTokenLifespan());
  }
}
