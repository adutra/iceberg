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
package org.apache.iceberg.rest.auth.oauth2.agent;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class ITOAuth2AgentLongBase {

  private static final Duration TEST_DURATION =
      Duration.parse(System.getProperty("rest.oauth2.test.long-test-duration", "PT30S"));

  /** Short delay for the fast agent. The delay is between 0 and 5 seconds. */
  private final LongSupplier shortDelay = () -> (long) (Math.random() * 5);

  /** Long delay for the slow agent. The delay is between 10 and 30 seconds. */
  private final LongSupplier longDelay = () -> 10 + (long) (Math.random() * 20);

  private ExecutorService executor;
  private CompletableFuture<Void> stop;

  @BeforeEach
  void before() {
    executor = ThreadPools.newFixedThreadPool("oauth2-long-tests", 1);
  }

  @AfterEach
  void after() {
    executor.shutdownNow();
  }

  protected void run(
      ImmutableTestEnvironment.Builder envBuilder1, ImmutableTestEnvironment.Builder envBuilder2)
      throws ExecutionException, InterruptedException {

    try (TestEnvironment env1 = envBuilder1.build();
        TestEnvironment env2 = envBuilder2.build();
        OAuth2Agent fast = env1.createAgent();
        OAuth2Agent slow = env2.createAgent()) {
      stop =
          CompletableFuture.runAsync(
              () -> {}, delayedExecutor(TEST_DURATION.toSeconds(), TimeUnit.SECONDS, executor));
      CompletableFuture<Void> future1 = schedule(fast, shortDelay);
      CompletableFuture<Void> future2 = schedule(slow, longDelay);
      stop.get();
      future1.complete(null);
      future2.complete(null);
      assertThat(future1).isNotCompletedExceptionally();
      assertThat(future2).isNotCompletedExceptionally();
    }
  }

  private CompletableFuture<Void> schedule(OAuth2Agent agent, LongSupplier nextDelay) {
    return CompletableFuture.runAsync(() -> authenticate(agent), executor)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                stop.completeExceptionally(error);
              }
            })
        .thenComposeAsync(
            v -> schedule(agent, nextDelay),
            delayedExecutor(nextDelay.getAsLong(), TimeUnit.SECONDS, executor));
  }

  protected abstract void authenticate(OAuth2Agent agent);
}
