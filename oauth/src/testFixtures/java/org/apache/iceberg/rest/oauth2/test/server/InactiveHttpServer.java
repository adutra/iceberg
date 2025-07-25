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

import java.net.URI;

/**
 * A no-op HttpServer implementation that does not support any operations.
 *
 * <p>This server is used for integration tests, since these tests run against a real authorization
 * server and do not need a mock server.
 */
public class InactiveHttpServer implements HttpServer {

  @Override
  public URI rootUrl() {
    throw new UnsupportedOperationException("Cannot get root URL of integration test server.");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Cannot reset integration test server.");
  }

  @Override
  public void close() {
    // nothing to do, the server is managed externally
  }
}
