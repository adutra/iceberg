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
package org.apache.iceberg.rest.oauth2.test.container;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

/** A test container for Nessie servers. */
public class NessieContainer extends GenericContainer<NessieContainer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieContainer.class);

  private URI baseUri;

  @SuppressWarnings("resource")
  public NessieContainer() {
    super("ghcr.io/projectnessie/nessie:0.104.1");
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
    withExposedPorts(19120, 9000);
    withNetworkAliases("nessie");
    waitingFor(Wait.forHttp("/q/health/ready").forPort(9000));
    withEnv("nessie.version.store.type", "IN_MEMORY");
    withEnv("quarkus.log.level", rootLoggerLevel());
    withEnv("quarkus.log.console.level", rootLoggerLevel());
    withEnv("quarkus.log.category.\"org.projectnessie\".level", nessieLoggerLevel());
  }

  @Override
  public void start() {
    super.start();
    baseUri = URI.create("http://localhost:" + getMappedPort(19120));
  }

  private static String rootLoggerLevel() {
    return LOGGER.isInfoEnabled() ? "INFO" : LOGGER.isWarnEnabled() ? "WARN" : "ERROR";
  }

  private static String nessieLoggerLevel() {
    return LOGGER.isDebugEnabled() ? "DEBUG" : rootLoggerLevel();
  }
}
