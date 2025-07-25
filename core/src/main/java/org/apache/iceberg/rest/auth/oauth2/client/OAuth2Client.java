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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtils;
import org.apache.iceberg.rest.auth.oauth2.flow.Flow;
import org.apache.iceberg.rest.auth.oauth2.flow.FlowFactory;
import org.apache.iceberg.rest.auth.oauth2.flow.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OAuth2 client is responsible for fetching and refreshing tokens, following the configuration
 * provided by an {@link OAuth2Config} object.
 */
public final class OAuth2Client implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Client.class);

  public static final String DEFAULT_CLIENT_NAME = "iceberg-oauth2-client";

  private static final Duration MIN_WARN_INTERVAL = Duration.ofSeconds(10);

  private static final CompletableFuture<TokensResult> MUST_FETCH_NEW_TOKENS_FUTURE =
      CompletableFuture.failedFuture(MustFetchNewTokensException.INSTANCE);

  private static final CompletableFuture<Void> DUMMY_COMPLETED_FUTURE =
      CompletableFuture.completedFuture(null);

  private final OAuth2Config config;
  private final ScheduledExecutorService executor;
  private final FlowFactory flowFactory;
  private final String name;
  private final Clock clock;

  private final CompletableFuture<Void> clientAccessed = new CompletableFuture<>();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean sleeping = new AtomicBoolean();

  private volatile CompletableFuture<TokensResult> currentTokensFuture;
  private volatile ScheduledFuture<?> tokenRefreshFuture;

  private volatile Instant lastAccess;
  private volatile Instant lastWarn;

  @SuppressWarnings("FutureReturnValueIgnored")
  public OAuth2Client(OAuth2Config config, OAuth2ClientRuntime runtime) {
    this.config = config;
    this.executor = runtime.executor();
    this.flowFactory = FlowFactory.create(config, runtime);
    name = config.basicConfig().clientName().orElse(DEFAULT_CLIENT_NAME);
    clock = runtime.clock();
    lastAccess = clock.instant();
    if (config.basicConfig().token().isPresent()) {
      TokensResult currentTokens = TokensResult.of(config.basicConfig().token().get());
      currentTokensFuture = CompletableFuture.completedFuture(currentTokens);
      maybeScheduleTokensRenewal(currentTokens);
    } else {
      // when user interaction is not required, token fetch can happen immediately;
      // otherwise, it will be deferred until authenticate() is called the first time,
      // in order to avoid bothering the user with a login prompt before the client is actually
      // used.
      boolean requiresUserInteraction =
          ConfigUtils.requiresUserInteraction(config.basicConfig().grantType());
      CompletableFuture<?> clientReady =
          requiresUserInteraction ? clientAccessed : DUMMY_COMPLETED_FUTURE;
      currentTokensFuture = clientReady.thenComposeAsync(v -> fetchNewTokens(), executor);
      currentTokensFuture
          .whenComplete(this::log)
          .whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
    }
  }

  /** Copy constructor. */
  @SuppressWarnings("FutureReturnValueIgnored")
  private OAuth2Client(OAuth2Client toCopy) {
    LOGGER.debug("[{}] Copying client", toCopy.name);
    config = toCopy.config;
    executor = toCopy.executor;
    flowFactory = toCopy.flowFactory.copy();
    name = toCopy.name;
    clock = toCopy.clock;
    lastAccess = toCopy.lastAccess;
    lastWarn = toCopy.lastWarn;
    tokenRefreshFuture = null;
    TokensResult currentTokens = getNow(toCopy.currentTokensFuture);
    currentTokensFuture =
        currentTokens != null
            ? CompletableFuture.completedFuture(currentTokens)
            : CompletableFuture.supplyAsync(this::fetchNewTokens, executor)
                .thenCompose(Function.identity());
    currentTokensFuture.whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
  }

  public OAuth2Config config() {
    return config;
  }

  /**
   * Creates a copy of this client. The copy will share the same config, executor and flow factory
   * as the original client, as well as its current tokens, if any. If token refresh is enabled, the
   * copy will create its own token refresh schedule.
   */
  public OAuth2Client copy() {
    return new OAuth2Client(this);
  }

  /**
   * Authenticates the client synchronously, waiting for the authentication to complete, and returns
   * the current access token. If the authentication fails, or if the client is closing, an
   * exception is thrown.
   */
  public AccessToken authenticate() {
    return authenticateInternal().tokens().getAccessToken();
  }

  /**
   * Authenticates the client asynchronously and returns a future that completes when the
   * authentication completes (either successfully or with an error).
   */
  public CompletionStage<AccessToken> authenticateAsync() {
    return authenticateAsyncInternal()
        .thenApply(TokensResult::tokens)
        .thenApply(Tokens::getAccessToken);
  }

  /**
   * Same as {@link #authenticate()} but returns the full {@link Tokens} object, including the
   * refresh token if any. Only intended for testing.
   */
  TokensResult authenticateInternal() {
    LOGGER.debug("[{}] Authenticating synchronously", name);
    onClientAccessed();
    return currentTokens();
  }

  /**
   * Same as {@link #authenticateAsync()} but returns the full {@link Tokens} object, including the
   * refresh token if any. Only intended for testing.
   */
  CompletionStage<TokensResult> authenticateAsyncInternal() {
    LOGGER.debug("[{}] Authenticating asynchronously", name);
    onClientAccessed();
    return currentTokensFuture;
  }

  TokensResult currentTokens() {
    try {
      Duration timeout = config.basicConfig().timeout();
      return currentTokensFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException("Timed out waiting for an access token", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof OAuth2Exception) {
        throw (OAuth2Exception) cause;
      } else {
        throw new RuntimeException("Cannot acquire a valid OAuth2 access token", cause);
      }
    }
  }

  @Override
  public void close() {
    if (closing.compareAndSet(false, true)) {
      try (flowFactory) {
        LOGGER.debug("[{}] Closing...", name);
        cancel(tokenRefreshFuture);
        cancel(currentTokensFuture);
        // Note: cancelling the clientAccessed future also invalidates any pending log messages
        cancel(clientAccessed);
      } finally {
        tokenRefreshFuture = null;
        // Don't clear currentTokensFuture, we'll need it in case this client is copied.
        LOGGER.debug("[{}] Closed", name);
      }
    }
  }

  CompletionStage<TokensResult> fetchNewTokens() {
    Flow flow = flowFactory.newInitialFlow();
    LOGGER.debug("[{}] Fetching new access token using {}", name, flow.grantType());
    CompletionStage<TokensResult> newTokensStage = flow.fetchNewTokens();
    // If the flow requires user interaction, update the last access time once the flow completes,
    // in order to better reflect when the client was actually accessed for the last time.
    // This prevents the client from going to sleep too early when the user is interacting with it.
    return ConfigUtils.requiresUserInteraction(config.basicConfig().grantType())
        ? newTokensStage.whenComplete((tokens, error) -> lastAccess = clock.instant())
        : newTokensStage;
  }

  CompletionStage<TokensResult> refreshCurrentTokens(TokensResult currentTokens) {
    if (config.tokenRefreshConfig().grantType().equals(GrantType.REFRESH_TOKEN)) {
      RefreshToken refreshToken = currentTokens.tokens().getRefreshToken();
      if (refreshToken == null
          || currentTokens.refreshTokenExpired(
              clock.instant().plus(config.tokenRefreshConfig().safetyMargin()))) {
        LOGGER.debug("[{}] Must fetch new tokens, refresh token is null or expired", name);
        return MUST_FETCH_NEW_TOKENS_FUTURE;
      }
    }

    Flow flow = flowFactory.newRefreshFlow(currentTokens.tokens());
    LOGGER.debug("[{}] Refreshing tokens using {}", name, flow.grantType());
    return flow.fetchNewTokens();
  }

  private void log(@Nullable TokensResult newTokens, @Nullable Throwable error) {
    if (newTokens != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[{}] Successfully fetched new tokens", name);
        LOGGER.debug(
            "[{}] Access token expiration time: {}", name, newTokens.accessTokenExpirationTime());
      }
    } else if (!closing.get()) {
      Throwable cause = error;
      if (cause instanceof CompletionException) {
        cause = error.getCause();
      }

      if (cause instanceof RESTException) {
        // Don't include the stack trace if the error is a RESTException,
        // since it's not very useful and just clutters the logs.
        maybeWarn("[{}] Failed to fetch new tokens: {}", name, cause.toString());
      } else {
        maybeWarn("[{}] Failed to fetch new tokens", name, cause);
      }
    }
  }

  private void maybeScheduleTokensRenewal(@Nullable TokensResult currentTokens) {
    if (!config.tokenRefreshConfig().enabled()) {
      LOGGER.debug(
          "[{}] Client is not configured to keep tokens refreshed, skipping token renewal", name);
      return;
    }

    if (closing.get()) {
      LOGGER.debug("[{}] Not checking if token renewal is required, client is closing", name);
      return;
    }

    Instant now = clock.instant();
    Duration timeSinceLastAccess = Duration.between(lastAccess, now);
    boolean idle = timeSinceLastAccess.compareTo(config.tokenRefreshConfig().idleTimeout()) > 0;
    LOGGER.debug("[{}] Time since last access: {}, idle: {}", name, timeSinceLastAccess, idle);
    if (idle) {
      maybeSleep();
    } else {
      Duration delay = nextTokenRefresh(currentTokens, now);
      scheduleTokensRenewal(delay);
    }
  }

  private void scheduleTokensRenewal(Duration delay) {
    if (closing.get()) {
      LOGGER.debug("[{}] Not scheduling token renewal, client is closing", name);
      return;
    }

    LOGGER.debug("[{}] Scheduling token refresh in {}", name, delay);
    try {
      ScheduledFuture<?> refreshFuture =
          executor.schedule(this::renewTokens, delay.toMillis(), TimeUnit.MILLISECONDS);
      this.tokenRefreshFuture = refreshFuture;
      if (closing.get()) {
        // We raced with close(): cancel the future we just created and clear the field.
        cancel(refreshFuture);
        this.tokenRefreshFuture = null;
      }
    } catch (RejectedExecutionException e) {
      if (closing.get()) {
        // We raced with close(), ignore
        return;
      }

      maybeWarn("[{}] Failed to schedule next token renewal, forcibly sleeping", name);
      sleep();
    }
  }

  private Duration nextTokenRefresh(@Nullable TokensResult currentTokens, Instant now) {
    Duration minRefreshDelay = config.tokenRefreshConfig().minRefreshDelay();
    if (currentTokens == null || currentTokens.accessTokenExpired(now)) {
      return minRefreshDelay;
    }

    Instant expirationTime = currentTokens.accessTokenExpirationTime();
    if (expirationTime == null) {
      Duration defaultLifespan = config.tokenRefreshConfig().accessTokenLifespan();
      maybeWarn(
          "[{}] Access token has no expiration time, assuming lifespan of {}",
          name,
          defaultLifespan);
      expirationTime = now.plus(defaultLifespan);
    }

    Duration delay =
        Duration.between(now, expirationTime).minus(config.tokenRefreshConfig().safetyMargin());
    if (delay.compareTo(minRefreshDelay) < 0) {
      LOGGER.debug("[{}] Next refresh delay was too short: {}", name, delay);
      delay = minRefreshDelay;
    }

    return delay;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void renewTokens() {
    if (closing.get()) {
      LOGGER.debug("[{}] Not renewing tokens, client is closing", name);
      return;
    }

    CompletableFuture<TokensResult> oldTokensFuture = currentTokensFuture;
    CompletableFuture<TokensResult> newTokensFuture =
        oldTokensFuture
            // try refreshing the current access token, if any
            .thenCompose(this::refreshCurrentTokens)
            // if that fails, try fetching brand-new tokens
            // (note: exceptionallyCompose() would be better but it's Java 12+)
            .handle(
                (tokens, error) ->
                    error == null ? CompletableFuture.completedFuture(tokens) : fetchNewTokens())
            .thenCompose(Function.identity());
    currentTokensFuture = newTokensFuture;

    if (closing.get()) {
      // We raced with close(): cancel the future we just created.
      cancel(newTokensFuture);
    } else {
      newTokensFuture
          .whenComplete(this::log)
          .whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
    }
  }

  private void maybeSleep() {
    if (!config.tokenRefreshConfig().enabled()) {
      LOGGER.debug(
          "[{}] Client is not configured to keep tokens refreshed, not entering sleep", name);
      return;
    }

    sleep();
  }

  private void sleep() {
    sleeping.set(true);
    LOGGER.debug("[{}] Sleeping...", name);
  }

  private void onClientAccessed() {
    if (closing.get()) {
      throw new IllegalStateException("Client is closing");
    }

    clientAccessed.complete(null);
    Instant now = clock.instant();
    lastAccess = now;
    if (sleeping.compareAndSet(true, false)) {
      wakeUp(now);
    }
  }

  private void wakeUp(Instant now) {
    if (closing.get()) {
      LOGGER.debug("[{}] Not waking up, client is closing", name);
      return;
    }

    LOGGER.debug("[{}] Waking up...", name);
    TokensResult currentTokens = getNow(currentTokensFuture);
    if (currentTokens == null
        || currentTokens.accessTokenExpired(now.plus(config.tokenRefreshConfig().safetyMargin()))) {
      LOGGER.debug("[{}] Refreshing tokens immediately", name);
      renewTokens();
    } else {
      LOGGER.debug("[{}] Tokens are still valid, scheduling refresh", name);
      Duration delay = nextTokenRefresh(currentTokens, now);
      scheduleTokensRenewal(delay);
    }
  }

  @SuppressWarnings({"FutureReturnValueIgnored", "Slf4jConstantLogMessage"})
  private void maybeWarn(String message, Object... args) {
    if (LOGGER.isWarnEnabled()) {
      Instant now = clock.instant();
      Instant last = lastWarn;
      boolean shouldWarn =
          last == null || Duration.between(last, now).compareTo(MIN_WARN_INTERVAL) > 0;
      if (shouldWarn) {
        // defer logging until the client is used to avoid confusing log messages appearing
        // before the client is actually used
        clientAccessed.thenRun(() -> LOGGER.warn(message, args));
        lastWarn = now;
        return;
      }
    }

    LOGGER.debug(message, args);
  }

  @Nullable
  private static <T> T getNow(@Nullable CompletableFuture<T> future) {
    try {
      return future == null ? null : future.getNow(null);
    } catch (Exception e) {
      return null;
    }
  }

  private static void cancel(@Nullable Future<?> future) {
    if (future != null) {
      future.cancel(true);
    }
  }

  static class MustFetchNewTokensException extends RuntimeException {

    @SuppressWarnings("StaticAssignmentOfThrowable")
    private static final MustFetchNewTokensException INSTANCE = new MustFetchNewTokensException();

    private MustFetchNewTokensException() {
      super(null, null, false, false);
    }
  }
}
