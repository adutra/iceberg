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
package org.apache.iceberg.rest.auth.oauth2.flow;

import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.DeviceAccessTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8628">Device
 * Authorization Grant</a> flow.
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class DeviceCodeFlow extends AbstractFlow implements InitialFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceCodeFlow.class);

  interface Builder extends AbstractFlow.Builder<DeviceCodeFlow, Builder> {}

  @Override
  public GrantType grantType() {
    return GrantType.DEVICE_CODE;
  }

  @Value.Derived
  String agentName() {
    return spec().runtimeConfig().agentName();
  }

  @Value.Derived
  String msgPrefix() {
    return FlowUtils.msgPrefix(spec().runtimeConfig().agentName());
  }

  /**
   * A future that will complete when fresh tokens are eventually obtained after polling the token
   * endpoint.
   */
  @Value.Default
  @SuppressWarnings("FutureReturnValueIgnored")
  CompletableFuture<Tokens> getTokensFuture() {
    CompletableFuture<Tokens> future = new CompletableFuture<>();
    future.whenComplete((tokens, error) -> stopPolling());
    return future;
  }

  @SuppressWarnings("immutables:incompat")
  private volatile Duration pollInterval;

  @SuppressWarnings("immutables:incompat")
  private volatile Future<?> pollFuture;

  private void stopPolling() {
    LOGGER.debug("[{}] Device Auth Flow: closing", agentName());
    Future<?> future = this.pollFuture;
    if (future != null) {
      future.cancel(true);
    }

    this.pollFuture = null;
  }

  @Override
  public CompletionStage<Tokens> fetchNewTokens() {
    LOGGER.debug("[{}] Device Auth Flow: started", agentName());
    return invokeDeviceAuthEndpoint()
        .thenCompose(
            response -> {
              pollInterval = spec().deviceCodeConfig().pollInterval();
              checkPollInterval(response.intervalSeconds());
              @SuppressWarnings("resource")
              PrintStream console = spec().runtimeConfig().console();
              synchronized (console) {
                console.println();
                console.println(msgPrefix() + FlowUtils.OAUTH2_AGENT_TITLE);
                console.println(msgPrefix() + FlowUtils.OAUTH2_AGENT_OPEN_URL);
                console.println(msgPrefix() + response.verificationUri());
                console.println(msgPrefix() + "And enter the code:");
                console.println(msgPrefix() + response.userCode());
                printExpirationNotice(response.expiresInSeconds());
                console.println();
                console.flush();
              }

              pollFuture = executor().submit(() -> pollForNewTokens(response.deviceCode()));
              return getTokensFuture();
            });
  }

  private void checkPollInterval(Integer serverPollInterval) {
    boolean ignoreServerPollInterval = spec().deviceCodeConfig().ignoreServerPollInterval();
    if (!ignoreServerPollInterval
        && serverPollInterval != null
        && serverPollInterval > pollInterval.getSeconds()) {
      LOGGER.debug(
          "[{}] Device Auth Flow: server requested minimum poll interval of {} seconds",
          agentName(),
          serverPollInterval);
      pollInterval = Duration.ofSeconds(serverPollInterval);
    }
  }

  private void printExpirationNotice(int seconds) {
    String exp;
    if (seconds < 60) {
      exp = seconds + " seconds";
    } else if (seconds % 60 == 0) {
      exp = seconds / 60 + " minutes";
    } else {
      exp = seconds / 60 + " minutes and " + seconds % 60 + " seconds";
    }

    @SuppressWarnings("resource")
    PrintStream console = spec().runtimeConfig().console();
    console.println(msgPrefix() + "(The code will expire in " + exp + ")");
  }

  private void pollForNewTokens(String deviceCode) {
    LOGGER.debug("[{}] Device Auth Flow: polling for new tokens", agentName());
    DeviceAccessTokenRequest.Builder request =
        DeviceAccessTokenRequest.builder().deviceCode(deviceCode);
    invokeTokenEndpoint(request, DefaultTokenResponse.class)
        .whenComplete(
            (tokens, error) -> {
              if (error == null) {
                LOGGER.debug("[{}] Device Auth Flow: new tokens received", agentName());
                getTokensFuture().complete(tokens);
              } else {
                Throwable maybeOAuth2Exception = error;
                if (error instanceof CompletionException) {
                  maybeOAuth2Exception = error.getCause();
                }

                if (maybeOAuth2Exception instanceof OAuth2Exception) {
                  String type = ((OAuth2Exception) maybeOAuth2Exception).errorResponse().type();
                  switch (type) {
                    case "authorization_pending":
                      LOGGER.debug(
                          "[{}] Device Auth Flow: waiting for authorization to complete",
                          agentName());
                      pollFuture =
                          executor()
                              .schedule(
                                  () -> pollForNewTokens(deviceCode),
                                  pollInterval.toMillis(),
                                  TimeUnit.MILLISECONDS);
                      return;
                    case "slow_down":
                      LOGGER.debug(
                          "[{}] Device Auth Flow: server requested to slow down", agentName());
                      Duration interval = this.pollInterval;
                      boolean ignoreServerPollInterval =
                          spec().deviceCodeConfig().ignoreServerPollInterval();
                      if (!ignoreServerPollInterval) {
                        interval = interval.plus(interval);
                        this.pollInterval = interval;
                      }

                      pollFuture =
                          executor()
                              .schedule(
                                  () -> pollForNewTokens(deviceCode),
                                  interval.toMillis(),
                                  TimeUnit.MILLISECONDS);
                      return;
                    case "access_denied":
                    case "expired_token":
                    default:
                      getTokensFuture().completeExceptionally(error);
                  }
                } else {
                  getTokensFuture().completeExceptionally(error);
                }
              }
            });
  }
}
