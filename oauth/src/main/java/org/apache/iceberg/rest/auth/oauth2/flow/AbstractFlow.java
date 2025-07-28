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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.oauth2.auth.ClientAuthenticator;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtils;
import org.apache.iceberg.rest.auth.oauth2.endpoint.EndpointProvider;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.PostFormRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenRequest;
import org.apache.iceberg.rest.auth.oauth2.token.Tokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Infrastructure shared by all flows. */
abstract class AbstractFlow implements Flow {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFlow.class);

  abstract BasicConfig spec();

  abstract Clock clock();

  abstract ScheduledExecutorService executor();

  abstract RESTClient restClient();

  abstract EndpointProvider endpointProvider();

  abstract ClientAuthenticator clientAuthenticator();

  protected <RequestT extends TokenRequest> CompletionStage<Tokens> invokeTokenEndpoint(
      TokenRequest.Builder<RequestT, ?> builder) {
    return CompletableFuture.supplyAsync(
            () -> {
              URI tokenEndpoint = endpointProvider().resolvedTokenEndpoint();
              builder.extraParameters(spec().extraRequestParameters());
              ConfigUtils.scopesAsString(spec().scopes()).ifPresent(builder::scope);
              Map<String, String> headers = getHeaders();
              clientAuthenticator().authenticate(builder, headers);
              RequestT request = builder.build();
              request.validate();
              LOGGER.debug(
                  "Invoking token endpoint: headers: {} body: {}",
                  filterSensitiveData(headers),
                  request);
              @SuppressWarnings("resource")
              RESTClient client = restClient();
              return client.postForm(
                  tokenEndpoint.toString(),
                  request.asFormParameters(),
                  DefaultTokenResponse.class,
                  headers,
                  FlowErrorHandler.INSTANCE);
            },
            executor())
        .whenComplete(this::log)
        .thenApply(resp -> resp.asTokens(clock()));
  }

  private void log(RESTResponse response, Throwable error) {
    if (LOGGER.isDebugEnabled()) {
      if (error == null) {
        LOGGER.debug("Received response from token endpoint: {}", response);
      } else {
        LOGGER.debug("Error invoking token endpoint: {}", error.toString());
      }
    }
  }

  private static Map<String, String> getHeaders() {
    Map<String, String> headers = Maps.newHashMap();
    headers.put("Content-Type", PostFormRequest.CONTENT_TYPE);
    return headers;
  }

  private static Map<String, String> filterSensitiveData(Map<String, String> headers) {
    Map<String, String> redactedHeaders = Maps.newHashMap(headers);
    if (redactedHeaders.containsKey("Authorization")) {
      redactedHeaders.put("Authorization", "****");
    }

    return redactedHeaders;
  }

  interface Builder<F extends AbstractFlow, B extends Builder<F, B>> {

    @CanIgnoreReturnValue
    B spec(BasicConfig spec);

    B clock(Clock clock);

    @CanIgnoreReturnValue
    B executor(ScheduledExecutorService executor);

    @CanIgnoreReturnValue
    B restClient(RESTClient restClient);

    @CanIgnoreReturnValue
    B endpointProvider(EndpointProvider endpointProvider);

    @CanIgnoreReturnValue
    B clientAuthenticator(ClientAuthenticator clientAuthenticator);

    F build();
  }
}
