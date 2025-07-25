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
 * A simple HTTP server for testing purposes.
 *
 * <p>There are two implementations:
 *
 * <ul>
 *   <li>{@link MockHttpServer} is a mock server that runs in-memory and is used for unit tests. It
 *       uses MockServer under the hood.
 *   <li>{@link InactiveHttpServer} is a no-op server that is used when the tests don't need a mock
 *       HTTP server. This is especially the case for integration tests that run against a real
 *       authorization server.
 * </ul>
 */
public interface HttpServer extends AutoCloseable {

  URI rootUrl();

  void reset();

  @Override
  void close();
}
