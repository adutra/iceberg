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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.device.DeviceAuthorizationErrorResponse;
import com.nimbusds.oauth2.sdk.device.DeviceAuthorizationRequest;
import com.nimbusds.oauth2.sdk.device.DeviceAuthorizationResponse;
import com.nimbusds.oauth2.sdk.device.DeviceAuthorizationSuccessResponse;
import com.nimbusds.oauth2.sdk.device.DeviceCode;
import com.nimbusds.oauth2.sdk.device.DeviceCodeGrant;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8628">Device
 * Authorization Grant</a> flow.
 */
@Value.Immutable
abstract class DeviceCodeFlow extends FlowBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceCodeFlow.class);

  interface Builder extends FlowBase.Builder<DeviceCodeFlow, Builder> {}

  @Override
  public final GrantType grantType() {
    return GrantType.DEVICE_CODE;
  }

  @Value.Derived
  String msgPrefix() {
    return FlowBase.msgPrefix(clientName());
  }

  /**
   * A future that will complete when fresh tokens are eventually obtained after polling the token
   * endpoint.
   */
  @Value.Default
  @SuppressWarnings("FutureReturnValueIgnored")
  CompletableFuture<TokensResult> tokensFuture() {
    CompletableFuture<TokensResult> future = new CompletableFuture<>();
    future.whenComplete((tokens, error) -> stopPolling());
    return future;
  }

  @SuppressWarnings("immutables:incompat")
  private volatile Duration pollInterval;

  @SuppressWarnings("immutables:incompat")
  private volatile Future<?> pollFuture;

  private void stopPolling() {
    LOGGER.debug("[{}] Device Auth Flow: closing", clientName());
    Future<?> future = this.pollFuture;
    if (future != null) {
      future.cancel(true);
    }

    this.pollFuture = null;
  }

  @Override
  public CompletionStage<TokensResult> fetchNewTokens() {
    LOGGER.debug("[{}] Device Auth Flow: started", clientName());
    pollInterval = config().deviceCodeConfig().pollInterval();
    return invokeDeviceAuthorizationEndpoint()
        .thenCompose(
            response -> {
              checkPollInterval(response.getInterval());
              @SuppressWarnings("resource")
              PrintStream console = runtime().console();
              synchronized (console) {
                console.println();
                console.println(msgPrefix() + OAUTH2_CLIENT_TITLE);
                console.println(msgPrefix() + OAUTH2_CLIENT_OPEN_URL);
                console.println(msgPrefix() + response.getVerificationURI());
                console.println(msgPrefix() + "And enter the code:");
                console.println(msgPrefix() + response.getUserCode().getValue());
                printExpirationNotice(response.getLifetime());
                console.println();
                console.flush();
              }

              pollFuture =
                  runtime().executor().submit(() -> pollForNewTokens(response.getDeviceCode()));
              return tokensFuture();
            });
  }

  private CompletionStage<DeviceAuthorizationSuccessResponse> invokeDeviceAuthorizationEndpoint() {
    DeviceAuthorizationRequest.Builder builder =
        publicClient()
            ? new DeviceAuthorizationRequest.Builder(clientId())
            : new DeviceAuthorizationRequest.Builder(createClientAuthentication());
    builder.endpointURI(endpointProvider().resolvedDeviceAuthorizationEndpoint());
    config().basicConfig().scope().ifPresent(builder::scope);
    config().basicConfig().extraRequestParameters().forEach(builder::customParameter);
    HTTPRequest request = builder.build().toHTTPRequest();
    return CompletableFuture.supplyAsync(() -> sendAndReceive(request), runtime().executor())
        .whenComplete((response, error) -> log(request, response, error))
        .thenApply(this::parseDeviceAuthorizationResponse);
  }

  private DeviceAuthorizationSuccessResponse parseDeviceAuthorizationResponse(
      HTTPResponse httpResponse) {
    try {
      DeviceAuthorizationResponse response = DeviceAuthorizationResponse.parse(httpResponse);
      if (!response.indicatesSuccess()) {
        DeviceAuthorizationErrorResponse errorResponse = response.toErrorResponse();
        throw new OAuth2Exception(errorResponse);
      }

      return response.toSuccessResponse();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkPollInterval(long serverPollInterval) {
    boolean ignoreServerPollInterval = config().deviceCodeConfig().ignoreServerPollInterval();
    if (!ignoreServerPollInterval && serverPollInterval > pollInterval.getSeconds()) {
      LOGGER.debug(
          "[{}] Device Auth Flow: server requested minimum poll interval of {} seconds",
          clientName(),
          serverPollInterval);
      pollInterval = Duration.ofSeconds(serverPollInterval);
    }
  }

  private void printExpirationNotice(long seconds) {
    String exp;
    if (seconds < 60) {
      exp = seconds + " seconds";
    } else if (seconds % 60 == 0) {
      exp = seconds / 60 + " minutes";
    } else {
      exp = seconds / 60 + " minutes and " + seconds % 60 + " seconds";
    }

    @SuppressWarnings("resource")
    PrintStream console = runtime().console();
    console.println(msgPrefix() + "(The code will expire in " + exp + ")");
  }

  private void pollForNewTokens(DeviceCode deviceCode) {
    LOGGER.debug("[{}] Device Auth Flow: polling for new tokens", clientName());
    invokeTokenEndpoint(new DeviceCodeGrant(deviceCode))
        .whenComplete(
            (tokens, error) -> {
              if (error == null) {
                LOGGER.debug("[{}] Device Auth Flow: new tokens received", clientName());
                tokensFuture().complete(tokens);
              } else {
                Throwable cause = error;
                if (cause instanceof CompletionException) {
                  cause = error.getCause();
                }

                if (cause instanceof OAuth2Exception) {
                  switch (((OAuth2Exception) cause).errorObject().getCode()) {
                    case "authorization_pending":
                      LOGGER.debug(
                          "[{}] Device Auth Flow: waiting for authorization to complete",
                          clientName());
                      pollFuture =
                          runtime()
                              .executor()
                              .schedule(
                                  () -> pollForNewTokens(deviceCode),
                                  pollInterval.toMillis(),
                                  TimeUnit.MILLISECONDS);
                      return;
                    case "slow_down":
                      LOGGER.debug(
                          "[{}] Device Auth Flow: server requested to slow down", clientName());
                      Duration interval = this.pollInterval;
                      boolean ignoreServerPollInterval =
                          config().deviceCodeConfig().ignoreServerPollInterval();
                      if (!ignoreServerPollInterval) {
                        interval = interval.plus(interval);
                        this.pollInterval = interval;
                      }

                      pollFuture =
                          runtime()
                              .executor()
                              .schedule(
                                  () -> pollForNewTokens(deviceCode),
                                  interval.toMillis(),
                                  TimeUnit.MILLISECONDS);
                      return;
                    case "access_denied":
                    case "expired_token":
                    default:
                      tokensFuture().completeExceptionally(cause);
                  }
                } else {
                  tokensFuture().completeExceptionally(cause);
                }
              }
            });
  }
}
