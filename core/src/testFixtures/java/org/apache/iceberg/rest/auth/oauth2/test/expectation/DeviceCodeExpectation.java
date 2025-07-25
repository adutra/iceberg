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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.device.DeviceCode;
import com.nimbusds.oauth2.sdk.device.UserCode;
import com.nimbusds.oauth2.sdk.id.Identifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;
import org.mockserver.model.StringBody;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class DeviceCodeExpectation extends TokenEndpointExpectation {

  /** A map of pending authorization requests, keyed by the user and device code. */
  @Value.Lazy
  protected ConcurrentMap<Identifier, PendingAuthRequest> pendingAuthRequests() {
    return Maps.newConcurrentMap();
  }

  @Override
  public void create() {
    createDeviceAuthEndpointExpectation();
    createDeviceVerificationEndpointExpectation();
    mockServer()
        .when(request())
        .respond(httpRequest -> response(httpRequest, "access_initial", "refresh_initial"));
  }

  @Override
  protected ImmutableMap.Builder<String, String> requestBody() {
    return super.requestBody()
        .put("grant_type", GrantType.DEVICE_CODE.getValue())
        .put("device_code", "[a-zA-Z0-9-._~]+");
  }

  @Override
  protected HttpResponse response(
      HttpRequest httpRequest, String accessToken, String refreshToken) {
    Map<String, List<String>> params = decodeBodyParameters(httpRequest);
    DeviceCode deviceCode = new DeviceCode(params.get("device_code").get(0));
    PendingAuthRequest pendingAuthRequest = pendingAuthRequests().get(deviceCode);
    if (pendingAuthRequest.userCodeReceived()) {
      pendingAuthRequests().remove(pendingAuthRequest.deviceCode());
      pendingAuthRequests().remove(pendingAuthRequest.userCode());
      return super.response(httpRequest, accessToken, refreshToken);
    } else {
      return HttpResponse.response()
          .withStatusCode(401)
          .withBody(
              JsonBody.json(
                  "{\"error\":\"authorization_pending\",\"error_description\":\"User code not yet received\"}"));
    }
  }

  private void createDeviceAuthEndpointExpectation() {
    mockServer()
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath(testEnvironment().deviceAuthorizationEndpoint().getPath())
                .withHeader("Content-Type", "application/x-www-form-urlencoded(; charset=UTF-8)?")
                .withBody(ParameterBody.params(Parameter.param("scope", ACCEPTED_SCOPES))))
        .respond(
            httpRequest -> {
              UserCode userCode = new UserCode();
              DeviceCode deviceCode = new DeviceCode();
              var pendingAuthRequest = new PendingAuthRequest(userCode, deviceCode);
              pendingAuthRequests().put(userCode, pendingAuthRequest);
              pendingAuthRequests().put(deviceCode, pendingAuthRequest);
              return HttpResponse.response()
                  .withBody(
                      JsonBody.json(
                          Map.of(
                              "device_code",
                              deviceCode.getValue(),
                              "user_code",
                              userCode.getValue(),
                              "verification_uri",
                              testEnvironment().deviceVerificationEndpoint().toString(),
                              "verification_uri_complete",
                              testEnvironment().deviceVerificationEndpoint().toString(),
                              "expires_in",
                              300,
                              "interval",
                              1)));
            });
  }

  private void createDeviceVerificationEndpointExpectation() {
    String path = testEnvironment().deviceVerificationEndpoint().getPath();
    // Expect the device verification page to be opened in a browser
    mockServer()
        .when(HttpRequest.request().withMethod("GET").withPath(path))
        .respond(
            HttpResponse.response()
                .withBody(
                    // Send a dummy HTML page to simulate the user interaction;
                    // the actual content is not important for the test.
                    StringBody.exact(
                        "<html><body>Enter device code:"
                            + "<form method=\"POST\" action=\""
                            + path
                            + "\">"
                            + "<input type=\"text\" name=\"device_user_code\" />"
                            + "<input type=\"submit\" value=\"Submit\" />"
                            + "</form>"
                            + "</body></html>",
                        MediaType.TEXT_HTML)));
    // Expect the device verification code to be sent by the user after opening the page
    mockServer()
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath(path)
                .withHeader("Content-Type", "application/x-www-form-urlencoded(; charset=UTF-8)?")
                .withBody(
                    ParameterBody.params(Parameter.param("device_user_code", "[a-zA-Z0-9-._~]+"))))
        .respond(
            httpRequest -> {
              // See https://github.com/mock-server/mockserver/issues/1468
              Map<String, List<String>> params = decodeBodyParameters(httpRequest);
              List<String> userCode = params.get("device_user_code");
              if (userCode == null || userCode.isEmpty()) {
                return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
              }

              PendingAuthRequest pendingAuthRequest =
                  pendingAuthRequests().get(new UserCode(userCode.get(0)));
              if (pendingAuthRequest == null) {
                return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
              }

              pendingAuthRequest.setUserCodeReceived(true);
              return HttpResponse.response()
                  .withBody(
                      StringBody.exact(
                          "<html><body>Device authorized</body></html>", MediaType.TEXT_HTML));
            });
  }

  public static final class PendingAuthRequest {

    private final UserCode userCode;
    private final DeviceCode deviceCode;

    private volatile boolean userCodeReceived;

    public PendingAuthRequest(UserCode userCode, DeviceCode deviceCode) {
      this.userCode = userCode;
      this.deviceCode = deviceCode;
    }

    public UserCode userCode() {
      return userCode;
    }

    public DeviceCode deviceCode() {
      return deviceCode;
    }

    public boolean userCodeReceived() {
      return userCodeReceived;
    }

    public void setUserCodeReceived(boolean userCodeReceived) {
      this.userCodeReceived = userCodeReceived;
    }
  }
}
