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

import java.net.URI;
import java.time.Duration;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

public class PolarisContainer extends GenericContainer<PolarisContainer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisContainer.class);

  private static final Duration ACCESS_TOKEN_LIFESPAN = Duration.ofSeconds(15);

  private URI baseUri;

  @SuppressWarnings("resource")
  public PolarisContainer() {
    super("apache/polaris:1.0.0-incubating");
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
    withExposedPorts(8181, 8182);
    waitingFor(
        new HttpWaitStrategy()
            .forPath("/q/health")
            .forPort(8182)
            .forResponsePredicate(body -> body.contains("\"status\": \"UP\"")));
    withEnv(
        "POLARIS_BOOTSTRAP_CREDENTIALS",
        "POLARIS," + TestConstants.CLIENT_ID1 + "," + TestConstants.CLIENT_SECRET1);
    withEnv("quarkus.log.level", rootLoggerLevel());
    withEnv("quarkus.log.category.\"io.quarkus.oidc\".level", polarisLoggerLevel());
    withEnv("quarkus.log.category.\"org.apache.polaris\".level", polarisLoggerLevel());
  }

  @Override
  public void start() {
    super.start();
    baseUri = URI.create("http://localhost:" + getMappedPort(8181));
  }

  public URI baseUri() {
    return baseUri;
  }

  public URI catalogApiEndpoint() {
    return baseUri.resolve("/api/catalog/");
  }

  public Duration accessTokenLifespan() {
    return ACCESS_TOKEN_LIFESPAN;
  }

  private static String rootLoggerLevel() {
    return LOGGER.isInfoEnabled() ? "INFO" : LOGGER.isWarnEnabled() ? "WARN" : "ERROR";
  }

  private static String polarisLoggerLevel() {
    return LOGGER.isDebugEnabled() ? "DEBUG" : rootLoggerLevel();
  }
}
