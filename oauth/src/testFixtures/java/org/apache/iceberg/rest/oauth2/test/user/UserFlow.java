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
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable that emulates a user browsing to the authorization URL printed on the console, then
 * following the instructions and optionally logging in with their credentials.
 *
 * <p>This implementation understands the HTML forms used by Keycloak.
 */
public abstract class UserFlow implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserFlow.class);

  /** The authorization URL to browse to. */
  protected abstract URI authUrl();

  /** The user behavior. */
  protected abstract UserBehavior userBehavior();

  /**
   * Callback to invoke when an error occurs. Allows signaling user flow failures back to the user
   * emulator thread.
   */
  protected abstract Consumer<Throwable> errorListener();

  protected static URI readRedirectUrl(HttpURLConnection conn) throws Exception {
    conn.setInstanceFollowRedirects(false);
    int responseCode = conn.getResponseCode();
    assertThat(responseCode)
        .isIn(
            HttpURLConnection.HTTP_MOVED_PERM,
            HttpURLConnection.HTTP_MOVED_TEMP,
            HttpURLConnection.HTTP_SEE_OTHER);
    String location = conn.getHeaderField("Location");
    assertThat(location).isNotNull();
    LOGGER.debug("Redirected to: {}", location);
    return URI.create(location);
  }
}
