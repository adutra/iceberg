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

import com.google.errorprone.annotations.FormatMethod;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Phaser;
import javax.annotation.Nullable;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtils;
import org.apache.iceberg.rest.auth.oauth2.config.PkceTransformation;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantType;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.rest.AuthorizationCodeTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.1">Authorization Code Grant</a>
 * flow.
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class AuthorizationCodeFlow extends AbstractFlow implements InitialFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationCodeFlow.class);

  private static final String HTML_TEMPLATE_OK =
      "<html><body><h1>Authentication successful</h1><p>You can close this page now.</p></body></html>";
  private static final String HTML_TEMPLATE_FAILED =
      "<html><body><h1>Authentication failed</h1><p>Could not obtain access token: %s</p></body></html>";

  private static final int STATE_LENGTH = 16;

  interface Builder extends AbstractFlow.Builder<AuthorizationCodeFlow, Builder> {}

  @Override
  public GrantType grantType() {
    return GrantType.AUTHORIZATION_CODE;
  }

  @Value.Derived
  String agentName() {
    return spec().runtimeConfig().agentName();
  }

  @Value.Derived
  String msgPrefix() {
    return FlowUtils.msgPrefix(spec().runtimeConfig().agentName());
  }

  @Value.Derived
  String state() {
    return FlowUtils.randomAlphaNumString(STATE_LENGTH);
  }

  @Value.Derived
  @Nullable
  String codeVerifier() {
    return spec().authorizationCodeConfig().pkceEnabled() ? FlowUtils.generateCodeVerifier() : null;
  }

  @Value.Derived
  String bindHost() {
    AuthorizationCodeConfig authorizationCodeConfig = spec().authorizationCodeConfig();
    return authorizationCodeConfig.callbackBindHost();
  }

  @Value.Derived
  int bindPort() {
    return spec().authorizationCodeConfig().callbackBindPort().orElse(0);
  }

  @Value.Derived
  String contextPath() {
    return spec()
        .authorizationCodeConfig()
        .callbackContextPath()
        .orElseGet(() -> FlowUtils.contextPath(spec().runtimeConfig().agentName()));
  }

  @Value.Derived
  HttpServer server() {
    return createServer(bindHost(), bindPort(), contextPath(), this::doRequest);
  }

  @Value.Derived
  URI redirectUri() {
    return spec()
        .authorizationCodeConfig()
        .redirectUri()
        .orElseGet(
            () -> defaultRedirectUri(bindHost(), server().getAddress().getPort(), contextPath()));
  }

  @Value.Derived
  URI authorizationUri() {
    URIBuilder authorizationUriBuilder =
        new URIBuilder(endpointProvider().resolvedAuthorizationEndpoint())
            .addParameter("response_type", "code")
            .addParameter("client_id", spec().basicConfig().clientId().orElseThrow())
            .addParameter(
                "scope", ConfigUtils.scopesAsString(spec().basicConfig().scopes()).orElse(null))
            .addParameter("redirect_uri", redirectUri().toString())
            .addParameter("state", state());
    if (spec().authorizationCodeConfig().pkceEnabled()) {
      PkceTransformation transformation = spec().authorizationCodeConfig().pkceTransformation();
      String codeChallenge = FlowUtils.generateCodeChallenge(transformation, codeVerifier());
      authorizationUriBuilder
          .addParameter("code_challenge", codeChallenge)
          .addParameter("code_challenge_method", transformation.canonicalName());
    }

    try {
      return authorizationUriBuilder.build().normalize();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to build authorization URI", e);
    }
  }

  /**
   * A future that will complete when the redirect URI is called for the first time. It will then
   * trigger the code extraction then the token fetching. Subsequent calls to the redirect URI will
   * not trigger any action. Note that the response to the redirect URI will be delayed until the
   * tokens are received.
   */
  @Value.Default
  CompletableFuture<HttpExchange> redirectUriFuture() {
    return new CompletableFuture<>();
  }

  /**
   * A future that will complete when the tokens are received in exchange for the authorization
   * code. Its completion will release all pending responses to the redirect URI. If the redirect
   * URI is called again after the tokens are received, the response will be immediate.
   */
  @Value.Derived
  @SuppressWarnings("FutureReturnValueIgnored")
  CompletableFuture<Tokens> tokensFuture() {
    CompletableFuture<Tokens> future =
        redirectUriFuture()
            .thenApply(this::extractAuthorizationCode)
            .thenCompose(this::fetchNewTokens)
            .whenComplete((tokens, error) -> log(error));
    future.whenCompleteAsync((tokens, error) -> stopServer(), executor());
    return future;
  }

  /**
   * A phaser that will delay closing the internal HTTP server until all inflight requests have been
   * processed. It is used to avoid closing the server prematurely and leaving the user's browser
   * with an aborted HTTP request.
   */
  @Value.Default
  Phaser inflightRequestsPhaser() {
    return new Phaser(1);
  }

  private void stopServer() {
    // Wait for all in-flight requests to complete before proceeding
    // (note: this call is potentially blocking!)
    inflightRequestsPhaser().arriveAndAwaitAdvance();
    LOGGER.debug("[{}] Authorization Code Flow: closing", agentName());
    server().stop(0);
  }

  @Override
  public CompletionStage<Tokens> fetchNewTokens() {
    LOGGER.debug(
        "[{}] Authorization Code Flow: started, redirect URI: {}", agentName(), redirectUri());
    @SuppressWarnings("resource")
    PrintStream console = spec().runtimeConfig().console();
    synchronized (console) {
      console.println();
      console.println(msgPrefix() + FlowUtils.OAUTH2_AGENT_TITLE);
      console.println(msgPrefix() + FlowUtils.OAUTH2_AGENT_OPEN_URL);
      console.println(msgPrefix() + authorizationUri());
      console.println();
      console.flush();
    }

    return tokensFuture();
  }

  /**
   * Handle the incoming HTTP request to the redirect URI. Since we are using the default executor,
   * which is a synchronous one, the very first invocation of this method will block the HTTP
   * server's dispatcher thread, until the authorization code is extracted and exchanged for tokens.
   * Subsequent requests will be processed immediately. The response to the request will be delayed
   * until the tokens are received.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void doRequest(HttpExchange exchange) {
    LOGGER.debug("[{}] Authorization Code Flow: received request", agentName());
    inflightRequestsPhaser().register();
    redirectUriFuture().complete(exchange); // will trigger the token fetching the first time
    tokensFuture()
        .handle((tokens, error) -> doResponse(exchange, error))
        .whenComplete((v, error) -> exchange.close())
        .whenComplete((v, error) -> inflightRequestsPhaser().arriveAndDeregister());
  }

  /** Send the response to the incoming HTTP request to the redirect URI. */
  private Void doResponse(HttpExchange exchange, Throwable error) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "[{}] Authorization Code Flow: sending response, error: {}",
          agentName(),
          error == null ? "none" : error.toString());
    }

    try {
      if (error == null) {
        writeResponse(exchange, HttpURLConnection.HTTP_OK, HTML_TEMPLATE_OK);
      } else {
        writeResponse(
            exchange, HttpURLConnection.HTTP_UNAUTHORIZED, HTML_TEMPLATE_FAILED, error.toString());
      }
    } catch (IOException e) {
      LOGGER.debug("[{}] Authorization Code Flow: error writing response", agentName(), e);
    }

    return null;
  }

  private String extractAuthorizationCode(HttpExchange exchange) {
    LOGGER.debug("[{}] Authorization Code Flow: extracting code", agentName());
    URIBuilder uriBuilder = new URIBuilder(exchange.getRequestURI());
    NameValuePair state = uriBuilder.getFirstQueryParam("state");
    if (state == null || !Objects.equals(state.getValue(), state())) {
      throw new IllegalArgumentException("Missing or invalid state");
    }

    NameValuePair code = uriBuilder.getFirstQueryParam("code");
    if (code == null || code.getValue() == null) {
      throw new IllegalArgumentException("Missing or invalid authorization code");
    }

    return code.getValue();
  }

  private CompletionStage<Tokens> fetchNewTokens(String code) {
    LOGGER.debug("[{}] Authorization Code Flow: fetching new tokens", agentName());
    AuthorizationCodeTokenRequest.Builder request =
        AuthorizationCodeTokenRequest.builder().code(code).redirectUri(redirectUri());
    String codeVerifier = codeVerifier();
    if (codeVerifier != null) {
      request.codeVerifier(codeVerifier);
    }

    return invokeTokenEndpoint(request, DefaultTokenResponse.class);
  }

  private void log(Throwable error) {
    if (LOGGER.isDebugEnabled()) {
      if (error == null) {
        LOGGER.debug("[{}] Authorization Code Flow: tokens received", agentName());
      } else {
        LOGGER.debug(
            "[{}] Authorization Code Flow: error fetching tokens: {}",
            agentName(),
            error.toString());
      }
    }
  }

  @SuppressWarnings("HttpUrlsUsage")
  private static URI defaultRedirectUri(String bindHost, int bindPort, String contextPath) {
    return URI.create(
            String.format(Locale.ROOT, "http://%s:%d/%s", bindHost, bindPort, contextPath))
        .normalize();
  }

  @SuppressWarnings("DnsLookup")
  private static HttpServer createServer(
      String hostname, int port, String contextPath, HttpHandler handler) {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress(hostname, port), 0);
      server.createContext(contextPath, handler);
      server.start();
      return server;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @FormatMethod
  private static void writeResponse(
      HttpExchange exchange, int status, String htmlTemplate, Object... args) throws IOException {
    String html = String.format(htmlTemplate, args);
    exchange.getResponseHeaders().add("Content-Type", "text/html");
    exchange.sendResponseHeaders(status, html.length());
    exchange.getResponseBody().write(html.getBytes(StandardCharsets.UTF_8));
  }
}
