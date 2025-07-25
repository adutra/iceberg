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
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import com.nimbusds.oauth2.sdk.util.URLUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Phaser;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.apache.hc.core5.ssl.PrivateKeyStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.iceberg.rest.auth.oauth2.config.AuthorizationCodeConfig;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.1">Authorization Code Grant</a>
 * flow.
 */
@Value.Immutable
abstract class AuthorizationCodeFlow extends FlowBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationCodeFlow.class);

  private static final String HTML_TEMPLATE_OK =
      "<html><body><h1>Authentication successful</h1><p>You can close this page now.</p></body></html>";
  private static final String HTML_TEMPLATE_FAILED =
      "<html><body><h1>Authentication failed</h1><p>Could not obtain access token: %s</p></body></html>";

  interface Builder extends FlowBase.Builder<AuthorizationCodeFlow, Builder> {}

  @Override
  public final GrantType grantType() {
    return GrantType.AUTHORIZATION_CODE;
  }

  @Value.Derived
  String msgPrefix() {
    return FlowBase.msgPrefix(clientName());
  }

  @Value.Lazy
  State state() {
    return new State();
  }

  @Value.Lazy
  CodeVerifier codeVerifier() {
    return new CodeVerifier();
  }

  @Value.Derived
  String bindHost() {
    return config().authorizationCodeConfig().callbackBindHost().orElse("localhost");
  }

  @Value.Derived
  int bindPort() {
    return config().authorizationCodeConfig().callbackBindPort().orElse(0);
  }

  @Value.Derived
  String contextPath() {
    return config()
        .authorizationCodeConfig()
        .callbackContextPath()
        .orElseGet(() -> FlowBase.contextPath(clientName()));
  }

  /**
   * The internal HTTP server used to receive the redirect URI call.
   *
   * @implNote The server is started when the flow is created, and stopped when the tokens are
   *     received.
   */
  @Value.Derived // cannot be lazy, the server MUST be started when the flow is created
  HttpServer server() {
    return config()
        .authorizationCodeConfig()
        .redirectUri()
        .map(uri -> createServer(uri.getHost(), uri.getPort(), uri.getPath()))
        .orElseGet(() -> createServer(bindHost(), bindPort(), contextPath()));
  }

  @Value.Derived
  boolean ssl() {
    return config()
        .authorizationCodeConfig()
        .redirectUri()
        .map(uri -> uri.getScheme().equals("https"))
        .orElseGet(() -> config().authorizationCodeConfig().callbackHttps());
  }

  /** The redirect URI, resolved to the actual port number after the server has started. */
  @Value.Derived
  URI resolvedRedirectUri() {
    return config().authorizationCodeConfig().redirectUri().orElseGet(this::buildRedirectUri);
  }

  /** The request to the authorization endpoint that will be presented to the user. */
  @Value.Derived
  URI authorizationUri() {
    AuthorizationRequest.Builder builder =
        new AuthorizationRequest.Builder(ResponseType.CODE, clientId())
            .endpointURI(endpointProvider().resolvedAuthorizationEndpoint())
            .redirectionURI(resolvedRedirectUri())
            .state(state());
    config().basicConfig().scope().ifPresent(builder::scope);
    config().basicConfig().extraRequestParameters().forEach(builder::customParameter);
    if (config().authorizationCodeConfig().pkceEnabled()) {
      CodeChallengeMethod method = config().authorizationCodeConfig().codeChallengeMethod();
      builder.codeChallenge(codeVerifier(), method);
    }

    return builder.build().toURI();
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
  CompletableFuture<TokensResult> tokensFuture() {
    CompletableFuture<TokensResult> future =
        redirectUriFuture()
            .thenApply(this::extractAuthorizationCode)
            .thenCompose(this::fetchNewTokens)
            .whenComplete((tokens, error) -> log(error));
    future.whenCompleteAsync((tokens, error) -> stopServer(), runtime().executor());
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
    LOGGER.debug("[{}] Authorization Code Flow: closing", clientName());
    server().stop(0);
  }

  @Override
  public CompletionStage<TokensResult> fetchNewTokens() {
    LOGGER.debug(
        "[{}] Authorization Code Flow: started, redirect URI: {}",
        clientName(),
        resolvedRedirectUri());
    @SuppressWarnings("resource")
    PrintStream console = runtime().console();
    synchronized (console) {
      console.println();
      console.println(msgPrefix() + OAUTH2_CLIENT_TITLE);
      console.println(msgPrefix() + OAUTH2_CLIENT_OPEN_URL);
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
    LOGGER.debug("[{}] Authorization Code Flow: received request", clientName());
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
          clientName(),
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
      LOGGER.debug("[{}] Authorization Code Flow: error writing response", clientName(), e);
    }

    return null;
  }

  private AuthorizationCode extractAuthorizationCode(HttpExchange exchange) {
    LOGGER.debug("[{}] Authorization Code Flow: extracting code", clientName());
    Map<String, List<String>> params =
        URLUtils.parseParameters(exchange.getRequestURI().getRawQuery());
    List<String> states = params.getOrDefault("state", List.of());
    if (states.size() != 1 || !state().getValue().equals(states.get(0))) {
      throw new IllegalArgumentException("Missing or invalid state");
    }

    List<String> codes = params.getOrDefault("code", List.of());
    if (codes.size() != 1) {
      throw new IllegalArgumentException("Missing or invalid authorization code");
    }

    return new AuthorizationCode(codes.get(0));
  }

  private CompletionStage<TokensResult> fetchNewTokens(AuthorizationCode code) {
    LOGGER.debug("[{}] Authorization Code Flow: fetching new tokens", clientName());
    AuthorizationCodeGrant grant =
        config().authorizationCodeConfig().pkceEnabled()
            ? new AuthorizationCodeGrant(code, resolvedRedirectUri(), codeVerifier())
            : new AuthorizationCodeGrant(code, resolvedRedirectUri());
    return invokeTokenEndpoint(grant);
  }

  private void log(Throwable error) {
    if (LOGGER.isDebugEnabled()) {
      if (error == null) {
        LOGGER.debug("[{}] Authorization Code Flow: tokens received", clientName());
      } else {
        LOGGER.debug(
            "[{}] Authorization Code Flow: error fetching tokens: {}",
            clientName(),
            error.toString());
      }
    }
  }

  private URI buildRedirectUri() {
    String scheme = ssl() ? "https" : "http";
    String host = bindHost();
    int port = server().getAddress().getPort(); // resolved port
    String path = contextPath();
    try {
      return new URI(scheme, null, host, port, path, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("DnsLookup")
  private HttpServer createServer(String hostname, int port, String contextPath) {
    try {
      HttpServer server;
      if (ssl()) {
        server = HttpsServer.create(new InetSocketAddress(hostname, port), 0);
        ((HttpsServer) server)
            .setHttpsConfigurator(
                new HttpsConfigurator(createSslContext()) {
                  @Override
                  public void configure(HttpsParameters params) {
                    configureSslParams(getSSLContext(), params);
                  }
                });
      } else {
        server = HttpServer.create(new InetSocketAddress(hostname, port), 0);
      }

      server.createContext(contextPath.isEmpty() ? "/" : contextPath, this::doRequest);
      server.start();
      return server;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SSLContext createSslContext() throws Exception {
    AuthorizationCodeConfig config = config().authorizationCodeConfig();
    if (config.sslKeyStorePath().isPresent()) {
      PrivateKeyStrategy strategy = null;
      if (config.sslKeyStoreAlias().isPresent()) {
        String alias = config.sslKeyStoreAlias().get();
        strategy = (aliases, sslParameters) -> aliases.containsKey(alias) ? alias : null;
      }

      return SSLContextBuilder.create()
          .loadKeyMaterial(
              config.sslKeyStorePath().get(),
              config.sslKeyStorePassword().map(String::toCharArray).orElse(null),
              config.sslKeyStorePassword().map(String::toCharArray).orElse(new char[0]),
              strategy)
          .build();
    }

    return SSLContext.getDefault();
  }

  private void configureSslParams(SSLContext sslContext, HttpsParameters params) {
    SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
    AuthorizationCodeConfig config = config().authorizationCodeConfig();
    if (!config.sslProtocols().isEmpty()) {
      sslParameters.setProtocols(config.sslProtocols().toArray(new String[0]));
    }

    if (!config.sslCipherSuites().isEmpty()) {
      sslParameters.setCipherSuites(config.sslCipherSuites().toArray(new String[0]));
    }

    params.setSSLParameters(sslParameters);
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
