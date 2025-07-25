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
package org.apache.iceberg.rest.auth.oauth2.client;

import java.io.PrintStream;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;

/**
 * A runtime context for the OAuth2 client.
 *
 * <p>This component groups together client dependencies that are not part of the client's
 * configuration as provided by the user, but rather are provided by the environment.
 */
@Value.Immutable
public interface OAuth2ClientRuntime {

  static OAuth2ClientRuntime of(ScheduledExecutorService executor) {
    return ImmutableOAuth2ClientRuntime.builder().executor(executor).build();
  }

  /**
   * The executor to use for asynchronous operations. In production, this is generally provided by
   * {@link ThreadPools#authRefreshPool()}.
   */
  ScheduledExecutorService executor();

  /**
   * The clock to use for time-based operations. Defaults to the system clock. Mostly used for
   * testing.
   */
  @Value.Default
  default Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * The {@link PrintStream} to use for console output. Defaults to {@link System#out}. Mostly used
   * for testing.
   */
  @Value.Default
  default PrintStream console() {
    return System.out;
  }
}
