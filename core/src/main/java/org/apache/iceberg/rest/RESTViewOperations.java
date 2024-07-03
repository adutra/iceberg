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
package org.apache.iceberg.rest;

import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;

class RESTViewOperations implements ViewOperations {
  private final RESTClient client;
  private final String path;
  private final AuthSession authSession;
  private final Set<Endpoint> endpoints;
  private ViewMetadata current;

  RESTViewOperations(
      RESTClient client,
      String path,
      AuthSession authSession,
      ViewMetadata current,
      Set<Endpoint> endpoints) {
    Preconditions.checkArgument(null != current, "Invalid view metadata: null");
    this.client = client;
    this.path = path;
    this.authSession = authSession;
    this.current = current;
    this.endpoints = endpoints;
  }

  @Override
  public ViewMetadata current() {
    return current;
  }

  @Override
  public ViewMetadata refresh() {
    Endpoint.check(endpoints, Endpoint.V1_LOAD_VIEW);
    return updateCurrentMetadata(
        client.get(
            path,
            LoadViewResponse.class,
            ImmutableMap.of(),
            authSession,
            ErrorHandlers.viewErrorHandler()));
  }

  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_VIEW);
    // this is only used for replacing view metadata
    Preconditions.checkState(base != null, "Invalid base metadata: null");

    UpdateTableRequest request =
        UpdateTableRequest.create(
            null, UpdateRequirements.forReplaceView(base, metadata.changes()), metadata.changes());

    LoadViewResponse response =
        client.post(
            path,
            request,
            LoadViewResponse.class,
            ImmutableMap.of(),
            authSession,
            ErrorHandlers.viewCommitHandler());

    updateCurrentMetadata(response);
  }

  private ViewMetadata updateCurrentMetadata(LoadViewResponse response) {
    if (!Objects.equals(current.metadataFileLocation(), response.metadataLocation())) {
      this.current = response.metadata();
    }

    return current;
  }
}
