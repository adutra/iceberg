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
package org.apache.iceberg.rest.oauth2.test.expectation;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.oauth2.flow.FlowUtils;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.rest.ImmutableDeviceAccessTokenRequest;
import org.apache.iceberg.rest.oauth2.rest.ImmutableDeviceAuthorizationResponse;
import org.apache.iceberg.rest.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;
import org.mockserver.model.StringBody;

@Value.Immutable
@OAuth2ImmutableStyle
@SuppressWarnings("resource")
public abstract class DeviceCodeExpectation extends InitialTokenFetchExpectation {

  /** A map of pending authorization requests, keyed twice by both the user and the device code. */
  @Value.Lazy
  protected ConcurrentMap<String, PendingAuthRequest> pendingAuthRequests() {
    return Maps.newConcurrentMap();
  }

  @Override
  public void create() {
    createDeviceAuthEndpointExpectation();
    createDeviceVerificationEndpointExpectation();
    clientAndServer().when(tokenRequest()).respond(this::tokenResponse);
  }

  @Override
  protected PostFormRequest tokenRequestBody() {
    return ImmutableDeviceAccessTokenRequest.builder()
        .clientId(
            testEnvironment().privateClient()
                ? null
                : String.format("(%s|%s)", TestConstants.CLIENT_ID1, TestConstants.CLIENT_ID2))
        .deviceCode("\\w{8}-\\w{8}")
        .scope(String.format("(%s|%s)", TestConstants.SCOPE1, TestConstants.SCOPE2))
        .putExtraParameter("(extra1|extra2)", "(value1|value2)")
        .build();
  }

  private HttpResponse tokenResponse(HttpRequest httpRequest) {
    List<NameValuePair> params = ExpectationUtils.decodeBodyParameters(httpRequest);
    String deviceCode = ExpectationUtils.findFirstParameterByName(params, "device_code");
    if (deviceCode == null) {
      return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
    }

    PendingAuthRequest pendingAuthRequest = pendingAuthRequests().get(deviceCode);
    if (pendingAuthRequest == null) {
      return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
    }

    if (pendingAuthRequest.isUserCodeReceived()) {
      pendingAuthRequests().remove(pendingAuthRequest.getDeviceCode());
      pendingAuthRequests().remove(pendingAuthRequest.getUserCode());
      return tokenResponse("access_initial", "refresh_initial");
    } else {
      return HttpResponse.response()
          .withStatusCode(401)
          .withBody(
              JsonBody.json(
                  "{\"error\":\"authorization_pending\",\"error_description\":\"User code not yet received\"}"));
    }
  }

  private void createDeviceAuthEndpointExpectation() {
    clientAndServer()
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath(testEnvironment().deviceAuthorizationEndpoint().getPath())
                .withContentType(MediaType.APPLICATION_FORM_URLENCODED)
                .withBody(
                    ParameterBody.params(
                        Parameter.param(
                            "scope",
                            String.format("(%s|%s)", TestConstants.SCOPE1, TestConstants.SCOPE2)))))
        .respond(
            httpRequest -> {
              String userCode =
                  FlowUtils.randomAlphaNumString(4) + "-" + FlowUtils.randomAlphaNumString(4);
              String deviceCode =
                  FlowUtils.randomAlphaNumString(8) + "-" + FlowUtils.randomAlphaNumString(8);
              var pendingAuthRequest = new PendingAuthRequest(userCode, deviceCode);
              pendingAuthRequests().put(userCode, pendingAuthRequest);
              pendingAuthRequests().put(deviceCode, pendingAuthRequest);
              return HttpResponse.response()
                  .withBody(
                      ExpectationUtils.jsonBody(
                          ImmutableDeviceAuthorizationResponse.builder()
                              .deviceCode(deviceCode)
                              .userCode(userCode)
                              .verificationUri(testEnvironment().deviceVerificationEndpoint())
                              .verificationUriComplete(
                                  testEnvironment().deviceVerificationEndpoint())
                              .expiresInSeconds(300)
                              .intervalSeconds(1)
                              .build()));
            });
  }

  private void createDeviceVerificationEndpointExpectation() {
    String path = testEnvironment().deviceVerificationEndpoint().getPath();
    // Expect the device verification page to be opened in a browser
    clientAndServer()
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
    clientAndServer()
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath(path)
                .withContentType(MediaType.APPLICATION_FORM_URLENCODED)
                .withBody(
                    ParameterBody.params(Parameter.param("device_user_code", "\\w{4}-\\w{4}"))))
        .respond(
            httpRequest -> {
              // See https://github.com/mock-server/mockserver/issues/1468
              List<NameValuePair> params = ExpectationUtils.decodeBodyParameters(httpRequest);
              String userCode =
                  ExpectationUtils.findFirstParameterByName(params, "device_user_code");
              if (userCode == null || userCode.isEmpty()) {
                return ErrorExpectation.AUTHORIZATION_SERVER_ERROR_RESPONSE;
              }

              PendingAuthRequest pendingAuthRequest = pendingAuthRequests().get(userCode);
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

    private final String userCode;
    private final String deviceCode;

    private volatile boolean userCodeReceived;

    public PendingAuthRequest(String userCode, String deviceCode) {
      this.userCode = userCode;
      this.deviceCode = deviceCode;
    }

    public String getUserCode() {
      return userCode;
    }

    public String getDeviceCode() {
      return deviceCode;
    }

    public boolean isUserCodeReceived() {
      return userCodeReceived;
    }

    public void setUserCodeReceived(boolean userCodeReceived) {
      this.userCodeReceived = userCodeReceived;
    }
  }
}
