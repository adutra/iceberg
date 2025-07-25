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

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.util.ThreadPools;

public abstract class ITOAuth2ClientStressBase {

  private final Duration total =
      Duration.parse(System.getProperty("rest.auth.oauth2.stress-tests.total-duration", "PT30S"));

  private final LongSupplier shortDelay = () -> (long) (Math.random() * 5);
  private final LongSupplier longDelay = () -> 10 + (long) (Math.random() * 20);

  private CompletableFuture<Void> stop;

  protected void run(
      ImmutableTestEnvironment.Builder envBuilder1, ImmutableTestEnvironment.Builder envBuilder2)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env1 = envBuilder1.build();
         TestEnvironment env2 = envBuilder2.build();
         OAuth2Client fast = env1.newClient();
         OAuth2Client slow = env2.newClient()) {
      stop =
          CompletableFuture.runAsync(
              () -> {}, delayedExecutor(total.toSeconds(), SECONDS, ThreadPools.getWorkerPool()));
      CompletableFuture<Void> future1 = schedule(fast, shortDelay);
      CompletableFuture<Void> future2 = schedule(slow, longDelay);
      stop.get();
      future1.complete(null);
      future2.complete(null);
      assertThat(future1).isNotCompletedExceptionally();
      assertThat(future2).isNotCompletedExceptionally();
    }
  }

  private CompletableFuture<Void> schedule(OAuth2Client client, LongSupplier nextDelay) {
    return CompletableFuture.runAsync(() -> authenticate(client), ThreadPools.getWorkerPool())
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                stop.completeExceptionally(error);
              }
            })
        .thenComposeAsync(
            v -> schedule(client, nextDelay),
            delayedExecutor(nextDelay.getAsLong(), SECONDS, ThreadPools.getWorkerPool()));
  }

  protected abstract void authenticate(OAuth2Client client);
}
