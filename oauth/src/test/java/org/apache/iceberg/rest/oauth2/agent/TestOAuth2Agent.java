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
package org.apache.iceberg.rest.oauth2.agent;

import static org.apache.iceberg.rest.oauth2.test.TestConstants.ACCESS_TOKEN_EXPIRATION_TIME;
import static org.apache.iceberg.rest.oauth2.test.TestConstants.REFRESH_TOKEN_EXPIRATION_TIME;
import static org.apache.iceberg.rest.oauth2.test.TokenAssertions.assertTokens;
import static org.assertj.core.api.InstanceOfAssertFactories.ATOMIC_BOOLEAN;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.rest.oauth2.agent.OAuth2Agent.MustFetchNewTokensException;
import org.apache.iceberg.rest.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.test.TestClock;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.apache.iceberg.rest.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.oauth2.token.AccessToken;
import org.apache.iceberg.rest.oauth2.token.RefreshToken;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(SoftAssertionsExtension.class)
class TestOAuth2Agent {

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void testClientCredentials() {
    try (TestEnvironment env = TestEnvironment.builder().build();
        OAuth2Agent agent = env.createAgent()) {
      Tokens currentTokens = agent.authenticateInternal();
      assertTokens(currentTokens, "access_initial", "refresh_initial");
    }
  }

  @Test
  void testClientCredentialsUnauthorized() {
    try (TestEnvironment env = TestEnvironment.builder().clientId("WrongClient").build();
        OAuth2Agent agent = env.createAgent()) {
      soft.assertThatThrownBy(agent::authenticate)
          .asInstanceOf(throwable(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorResponse)
          .satisfies(
              r -> {
                soft.assertThat(r.type()).isEqualTo("invalid_request");
                soft.assertThat(r.message()).contains("Invalid request");
              });
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testPassword(boolean privateClient, boolean returnRefreshTokens) {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.PASSWORD)
                .privateClient(privateClient)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      Tokens currentTokens = agent.authenticateInternal();
      assertTokens(currentTokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }

  @Test
  void testPasswordUnauthorized() {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.PASSWORD)
                .password("WrongPassword")
                .build();
        OAuth2Agent agent = env.createAgent()) {
      soft.assertThatThrownBy(agent::authenticate)
          .asInstanceOf(throwable(OAuth2Exception.class))
          .extracting(OAuth2Exception::errorResponse)
          .satisfies(
              r -> {
                soft.assertThat(r.type()).isEqualTo("invalid_request");
                soft.assertThat(r.message()).contains("Invalid request");
              });
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "true, false", "false, true", "false, false"})
  void testRefreshToken(boolean privateClient, boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.PASSWORD)
                .privateClient(privateClient)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        OAuth2Agent agent = env.createAgent()) {
      Tokens currentTokens =
          Tokens.of(
              AccessToken.of("access_initial", "Bearer", ACCESS_TOKEN_EXPIRATION_TIME),
              RefreshToken.of("refresh_initial", REFRESH_TOKEN_EXPIRATION_TIME));
      Tokens tokens = agent.refreshCurrentTokens(currentTokens).toCompletableFuture().get();
      assertTokens(
          tokens,
          "access_refreshed",
          returnRefreshTokens ? "refresh_refreshed" : "refresh_initial");
    }
  }

  @Test
  void testRefreshTokenExpired() {
    try (TestEnvironment env = TestEnvironment.builder().build();
        OAuth2Agent agent = env.createAgent()) {
      Tokens currentTokens =
          Tokens.of(
              AccessToken.of("access_initial", "Bearer", ACCESS_TOKEN_EXPIRATION_TIME),
              RefreshToken.of("refresh_initial", REFRESH_TOKEN_EXPIRATION_TIME));
      Tokens tokens =
          Tokens.of(
              currentTokens.accessToken(),
              RefreshToken.of("refresh_expired", TestConstants.NOW.minusSeconds(1)));
      soft.assertThat(agent.refreshCurrentTokens(tokens))
          .completesExceptionallyWithin(Duration.ofSeconds(10))
          .withThrowableOfType(ExecutionException.class)
          .withCauseInstanceOf(MustFetchNewTokensException.class);
    }
  }

  @Test
  void testSleepWakeUp() {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    mockExecutorExecute(executor);
    AtomicReference<Runnable> currentRenewalTask = mockExecutorSchedule(executor);

    try (TestEnvironment env = TestEnvironment.builder().executor(executor).build();
        OAuth2Agent agent = env.createAgent()) {

      // should fetch the initial token
      AccessToken token = agent.authenticate();
      soft.assertThat(token.payload()).isEqualTo("access_initial");

      // emulate executor running the scheduled renewal task
      currentRenewalTask.get().run();

      // should have refreshed the token
      token = agent.authenticate();
      soft.assertThat(token.payload()).isEqualTo("access_refreshed");

      Duration idleTimeout = env.agentSpec().tokenRefreshConfig().idleTimeout().plusSeconds(1);

      // emulate executor running the scheduled renewal task and detecting that the agent is idle
      ((TestClock) env.clock()).plus(idleTimeout);
      currentRenewalTask.get().run();
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();

      // should exit sleeping mode on next authenticate() call and schedule a token refresh
      token = agent.authenticate();
      soft.assertThat(token.payload()).isEqualTo("access_refreshed");
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isFalse();

      // emulate executor running the scheduled renewal task and detecting that the agent is idle
      // again
      ((TestClock) env.clock()).plus(idleTimeout);
      currentRenewalTask.get().run();
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();

      // should exit sleeping mode on next authenticate() call
      // and refresh tokens immediately because the current ones are expired
      ((TestClock) env.clock()).plus(TestConstants.REFRESH_TOKEN_LIFESPAN);
      token = agent.authenticate();
      soft.assertThat(token.payload()).isEqualTo("access_initial");
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isFalse();
    }
  }

  @Test
  void testExecutionRejectedOnInitialTokenFetch() {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    // First token fetch will throw
    doThrow(RejectedExecutionException.class).when(executor).execute(any(Runnable.class));
    AtomicReference<Runnable> currentRenewalTask = mockExecutorSchedule(executor);

    // If the executor rejects the initial token fetch, a call to authenticate()
    // throws RejectedExecutionException immediately.

    try (TestEnvironment env = TestEnvironment.builder().executor(executor).build();
        OAuth2Agent agent = env.createAgent()) {

      soft.assertThatThrownBy(agent::authenticate)
          .isInstanceOf(RuntimeException.class)
          .hasCauseInstanceOf(RejectedExecutionException.class)
          .hasMessageContaining("Cannot acquire a valid OAuth2 access token");

      // Next token fetch will succeed
      mockExecutorExecute(executor);

      // should have scheduled a refresh, when that refresh is executed successfully,
      // agent should recover
      soft.assertThat(currentRenewalTask.get()).isNotNull();
      currentRenewalTask.get().run();
      agent.authenticate();
    }
  }

  @Test
  void testExecutionRejectedOnTokenRefreshes() {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    mockExecutorExecute(executor);
    AtomicReference<Runnable> currentRenewalTask = mockExecutorSchedule(executor, true);

    // If the executor rejects a scheduled token refresh,
    // sleep mode should be activated; the first call to authenticate()
    // will trigger wake up, then refresh the token immediately (synchronously) if necessary,
    // then schedule a new refresh. If that refresh is rejected again, sleep mode is reactivated.

    try (TestEnvironment env = TestEnvironment.builder().executor(executor).build();
        OAuth2Agent agent = env.createAgent()) {

      // will trigger token fetch (successful), then schedule a refresh, then reject it,
      // then sleep
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();
      soft.assertThat(currentRenewalTask.get()).isNull();

      // will wake up, then reject scheduling the next refresh, then sleep again,
      // then return the previously fetched token since it's still valid.
      AccessToken token = agent.authenticate();
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();
      soft.assertThat(token.payload()).isEqualTo("access_initial");
      soft.assertThat(currentRenewalTask.get()).isNull();

      // will wake up, then refresh the token immediately (since it's expired),
      // then reject scheduling the next refresh, then sleep again,
      // then return the newly-fetched token
      ((TestClock) env.clock()).plus(TestConstants.ACCESS_TOKEN_LIFESPAN);
      token = agent.authenticate();
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();
      soft.assertThat(token.payload()).isEqualTo("access_refreshed");
      soft.assertThat(currentRenewalTask.get()).isNull();
    }
  }

  @Test
  void testFailureRecovery() {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    mockExecutorExecute(executor);
    AtomicReference<Runnable> currentRenewalTask = mockExecutorSchedule(executor);

    try (TestEnvironment env =
            TestEnvironment.builder()
                .executor(executor)
                .createDefaultExpectations(false)
                .discoveryEnabled(false)
                .build();
        OAuth2Agent agent = env.createAgent()) {

      // simple failure recovery scenarios

      // Emulate failure on initial token fetch
      // => propagate the error but schedule a refresh ASAP
      env.createErrorExpectations();
      Runnable renewalTask = currentRenewalTask.get();
      soft.assertThat(renewalTask).isNotNull();
      soft.assertThatThrownBy(agent::authenticate)
          .isInstanceOf(OAuth2Exception.class)
          .hasMessageContaining("OAuth2 request failed");

      // Emulate executor running the scheduled refresh task, then throwing an exception
      // => propagate the error but schedule another refresh
      renewalTask.run();
      soft.assertThat(currentRenewalTask.get()).isNotNull().isNotSameAs(renewalTask);
      renewalTask = currentRenewalTask.get();
      soft.assertThatThrownBy(agent::authenticate)
          .isInstanceOf(OAuth2Exception.class)
          .hasMessageContaining("OAuth2 request failed");

      // Emulate executor running the scheduled refresh task again, then finally getting tokens
      // => should recover and return initial tokens + schedule next refresh
      env.reset();
      env.createExpectations();
      renewalTask.run();
      soft.assertThat(currentRenewalTask.get()).isNotNull().isNotSameAs(renewalTask);
      renewalTask = currentRenewalTask.get();
      AccessToken token = agent.authenticate();
      soft.assertThat(token.payload()).isEqualTo("access_initial");

      // failure recovery when in sleep mode

      Duration idleTimeout = env.agentSpec().tokenRefreshConfig().idleTimeout().plusSeconds(1);

      // Emulate executor running the scheduled refresh task again, getting tokens,
      // then setting sleeping to true because idle interval is past
      ((TestClock) env.clock()).plus(idleTimeout);
      renewalTask.run();
      soft.assertThat(currentRenewalTask.get()).isSameAs(renewalTask);
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();

      // Emulate waking up when current access token has expired,
      // then getting an error when renewing tokens immediately
      // => should propagate the error but schedule another refresh
      env.reset();
      env.createErrorExpectations();
      ((TestClock) env.clock()).plus(TestConstants.ACCESS_TOKEN_LIFESPAN);
      soft.assertThatThrownBy(agent::authenticate)
          .isInstanceOf(OAuth2Exception.class)
          .hasMessageContaining("Invalid request");
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isFalse();
      soft.assertThat(currentRenewalTask.get()).isNotNull().isNotSameAs(renewalTask);
      renewalTask = currentRenewalTask.get();

      // Emulate executor running the scheduled refresh task again,
      // then getting an error, then setting sleeping to true again because idle interval is past
      ((TestClock) env.clock()).plus(idleTimeout);
      renewalTask.run();
      soft.assertThat(currentRenewalTask.get()).isSameAs(renewalTask);
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();
      soft.assertThatThrownBy(agent::getCurrentTokens)
          .isInstanceOf(OAuth2Exception.class)
          .hasMessageContaining("Invalid request");

      // Emulate waking up, then fetching tokens immediately because no tokens are available,
      // then scheduling next refresh
      env.reset();
      env.createExpectations();
      token = agent.authenticate();
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isFalse();
      soft.assertThat(token.payload()).isEqualTo("access_initial");
      soft.assertThat(currentRenewalTask.get()).isNotSameAs(renewalTask);
      renewalTask = currentRenewalTask.get();

      // Emulate executor running the scheduled refresh task again, refreshing tokens,
      // then setting sleeping to true again because idle interval is past
      ((TestClock) env.clock()).plus(idleTimeout);
      renewalTask.run();
      soft.assertThat(currentRenewalTask.get()).isSameAs(renewalTask);
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isTrue();

      // Emulate waking up, then rescheduling a refresh since current token is still valid
      token = agent.authenticate();
      soft.assertThat(agent).extracting("sleeping", ATOMIC_BOOLEAN).isFalse();
      soft.assertThat(token.payload()).isEqualTo("access_refreshed");
      soft.assertThat(currentRenewalTask.get()).isNotSameAs(renewalTask);
    }
  }

  /** Mocks the executor's execute() method to run the task immediately. */
  private static void mockExecutorExecute(ScheduledExecutorService executor) {
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
  }

  /** Mocks the executor's schedule() method to capture the scheduled task. */
  private static AtomicReference<Runnable> mockExecutorSchedule(ScheduledExecutorService executor) {
    return mockExecutorSchedule(executor, false);
  }

  /**
   * Mocks the executor's schedule() method to capture the scheduled task, optionally rejecting it.
   */
  private static AtomicReference<Runnable> mockExecutorSchedule(
      ScheduledExecutorService executor, boolean rejectSchedule) {
    AtomicReference<Runnable> task = new AtomicReference<>();
    when(executor.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              if (rejectSchedule) {
                throw new RejectedExecutionException("test");
              }

              Runnable runnable = invocation.getArgument(0);
              task.set(runnable);
              return mock(ScheduledFuture.class);
            });
    return task;
  }
}
