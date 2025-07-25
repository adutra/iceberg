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
package org.apache.iceberg.rest.oauth2.test.server;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;

/**
 * A mock HTTP server that uses MockServer's {@link ClientAndServer} to implement the OAuth2
 * endpoints and different behaviors.
 *
 * <p>This server is used for unit tests only.
 */
public class MockHttpServer implements HttpServer {

  private static final AtomicInteger COUNTER = new AtomicInteger(1);

  private final ClientAndServer clientAndServer;

  public MockHttpServer() {
    Configuration configuration = Configuration.configuration();
    String outputDir = System.getProperty("rest.oauth2.test.mockserver.memoryUsageCsvDirectory");
    if (outputDir != null) {
      Path outputPath = Paths.get(outputDir).resolve("server-" + COUNTER.getAndIncrement());
      try {
        Files.createDirectories(outputPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      configuration.outputMemoryUsageCsv(true);
      configuration.memoryUsageCsvDirectory(outputPath.toString());
    }

    clientAndServer = ClientAndServer.startClientAndServer(configuration);
  }

  public ClientAndServer getClientAndServer() {
    return clientAndServer;
  }

  @Override
  public URI rootUrl() {
    return URI.create("http://localhost:" + clientAndServer.getLocalPort());
  }

  @Override
  public void reset() {
    clientAndServer.reset();
  }

  @Override
  public void close() {
    clientAndServer.close();
  }
}
