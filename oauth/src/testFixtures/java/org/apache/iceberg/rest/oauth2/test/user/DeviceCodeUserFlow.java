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
import java.util.Map;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A user flow that responds to Device Code flows. */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class DeviceCodeUserFlow extends UserFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceCodeUserFlow.class);

  protected abstract String userCode();

  @Override
  public void run() {
    try {
      LOGGER.debug("Starting device code user flow.");
      enterUserCode(authUrl(), userCode());
      LOGGER.debug("Device code user flow completed.");
    } catch (Exception | AssertionError t) {
      errorListener().accept(t);
    }
  }

  /** Emulates user entering provided user code on the authorization server. */
  private void enterUserCode(URI codePageUrl, String userCode) throws Exception {
    LOGGER.debug("Entering user code...");
    HttpURLConnection codeActionConn = (HttpURLConnection) codePageUrl.toURL().openConnection();
    if (userBehavior().emulateFailure()) {
      Map<String, String> data = Map.of("device_user_code", "wrong_code");
      postForm(codeActionConn, data);
      assertThat(codeActionConn.getResponseCode()).isEqualTo(HttpURLConnection.HTTP_UNAUTHORIZED);
    } else {
      Map<String, String> data = Map.of("device_user_code", userCode);
      postForm(codeActionConn, data);
      assertThat(codeActionConn.getResponseCode()).isEqualTo(HttpURLConnection.HTTP_OK);
    }

    codeActionConn.disconnect();
  }
}
