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
package org.apache.iceberg.rest.oauth2.test.user;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Set;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A user flow that responds to Authorization Code flows. This implementation is compatible with
 * unit test expectations as well as with Keycloak's behavior.
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class AuthorizationCodeUserFlow extends UserFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationCodeUserFlow.class);

  @Override
  public void run() {
    try {
      LOGGER.debug("Starting authorization code user flow.");
      Set<String> cookies = Sets.newHashSet();
      URI callbackUri;
      if (userBehavior().username().isEmpty()) {
        HttpURLConnection conn = (HttpURLConnection) authUrl().toURL().openConnection();
        callbackUri = readRedirectUrl(conn, cookies);
        conn.disconnect();
      } else {
        var username = userBehavior().requiredUsername();
        var password = userBehavior().requiredPassword();
        callbackUri = login(authUrl(), username, password, cookies);
      }

      invokeCallbackUrl(callbackUri);
      LOGGER.debug("Authorization code user flow completed.");
    } catch (Exception | AssertionError t) {
      errorListener().accept(t);
    }
  }

  /** Emulate browser being redirected to callback URL. */
  private void invokeCallbackUrl(URI callbackUrl) throws Exception {
    LOGGER.debug("Opening callback URL...");
    assertThat(callbackUrl).hasParameter("code").hasParameter("state");
    boolean useWrongCode = userBehavior().emulateFailure();
    HttpURLConnection conn;
    if (useWrongCode) {
      NameValuePair state = new URIBuilder(callbackUrl).getFirstQueryParam("state");
      URI newCallbackUrl =
          new URIBuilder(callbackUrl)
              .clearParameters()
              .addParameter("code", "WRONG-CODE")
              .addParameter(state)
              .build();
      conn = (HttpURLConnection) newCallbackUrl.toURL().openConnection();
    } else {
      conn = (HttpURLConnection) callbackUrl.toURL().openConnection();
    }

    conn.setRequestMethod("GET");
    int status = conn.getResponseCode();
    conn.disconnect();
    assertThat(status)
        .isEqualTo(useWrongCode ? HttpURLConnection.HTTP_UNAUTHORIZED : HttpURLConnection.HTTP_OK);
  }
}
