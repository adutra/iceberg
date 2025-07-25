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
package org.apache.iceberg.rest.auth.oauth2.test.user;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.annotation.Nullable;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A user interaction for Device Code flows.
 *
 * <p>This implementation is compatible with unit tests and integration tests against Keycloak.
 */
@Value.Immutable
public abstract class DeviceCodeUserInteraction extends UserInteraction {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceCodeUserInteraction.class);

  private static final Pattern FORM_ACTION_PATTERN =
      Pattern.compile("<form.*action=\"([^\"]+)\".*>");

  private static final Pattern HIDDEN_CODE_PATTERN =
      Pattern.compile("<input type=\"hidden\" name=\"code\" value=\"([^\"]+)\">");

  protected abstract String userCode();

  @Override
  public void run() {
    try {
      LOGGER.debug("Starting device code user interaction.");
      Set<String> cookies = Sets.newHashSet();
      URI loginPageUrl = enterUserCode(authUrl(), userCode(), cookies);
      if (loginPageUrl != null) {
        var username = userBehavior().requiredUsername();
        var password = userBehavior().requiredPassword();
        URI consentPageUrl = login(loginPageUrl, username, password, cookies);
        authorizeDevice(consentPageUrl, cookies);
      }

      LOGGER.debug("Device code user interaction completed.");
    } catch (Exception | AssertionError t) {
      errorListener().accept(t);
    }
  }

  /** Emulates user entering provided user code on the authorization server. */
  @Nullable
  private URI enterUserCode(URI codePageUrl, String userCode, Set<String> cookies)
      throws Exception {
    LOGGER.debug("Entering user code...");
    // receive device code page (and discard the HTML content)
    htmlPage(codePageUrl, cookies);
    // send device code form to same URL but with POST
    HttpURLConnection codeActionConn = openConnection(codePageUrl);
    // Emulate a failure at this step for unit tests only; for integration tests, we'll do it later
    boolean wrongCode = userBehavior().emulateFailure() && userBehavior().username().isEmpty();
    Map<String, String> data =
        ImmutableMap.of("device_user_code", wrongCode ? "wrong_code" : userCode);
    postForm(codeActionConn, data, cookies);
    URI loginUrl = null;
    if (wrongCode) {
      assertThat(codeActionConn.getResponseCode()).isEqualTo(HttpURLConnection.HTTP_UNAUTHORIZED);
    } else {
      if (userBehavior().username().isEmpty()) {
        // Unit tests: expect just a 200 OK
        assertThat(codeActionConn.getResponseCode()).isEqualTo(HttpURLConnection.HTTP_OK);
      } else {
        // Expect a redirect to the login page
        loginUrl = readRedirectUrl(codeActionConn, cookies);
      }
    }

    codeActionConn.disconnect();
    return loginUrl;
  }

  /** Emulates user consenting to authorize device on the authorization server. */
  private void authorizeDevice(URI consentPageUrl, Set<String> cookies) throws Exception {
    LOGGER.debug("Authorizing device...");
    // receive consent page
    String consentHtml = htmlPage(consentPageUrl, cookies);
    Matcher matcher = FORM_ACTION_PATTERN.matcher(consentHtml);
    assertThat(matcher.find()).isTrue();
    URI formAction = URI.create(matcher.group(1));
    matcher = HIDDEN_CODE_PATTERN.matcher(consentHtml);
    assertThat(matcher.find()).isTrue();
    String deviceCode = matcher.group(1);
    // send consent form
    URI consentActionUrl =
        new URI(
            consentPageUrl.getScheme(),
            null,
            consentPageUrl.getHost(),
            consentPageUrl.getPort(),
            formAction.getPath(),
            formAction.getQuery(),
            null);
    HttpURLConnection consentActionConn = openConnection(consentActionUrl);
    boolean denyConsent = userBehavior().emulateFailure();
    Map<String, String> data =
        denyConsent
            ? ImmutableMap.of("code", deviceCode, "cancel", "No")
            : ImmutableMap.of("code", deviceCode, "accept", "Yes");
    postForm(consentActionConn, data, cookies);
    // Read the response but discard it, as it points to a static success HTML page
    readRedirectUrl(consentActionConn, cookies);
    consentActionConn.disconnect();
  }
}
