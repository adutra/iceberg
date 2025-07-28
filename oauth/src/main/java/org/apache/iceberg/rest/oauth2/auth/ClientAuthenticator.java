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
package org.apache.iceberg.rest.oauth2.auth;

import java.util.Map;
import org.apache.iceberg.rest.oauth2.rest.ClientRequest;
import org.apache.iceberg.rest.oauth2.rest.ClientRequest.Builder;

/**
 * A client authenticator. This interface is used to authenticate a client by adding the necessary
 * authentication information to the request.
 */
public interface ClientAuthenticator {

  /**
   * Authenticates a client by adding the necessary authentication information to the request.
   *
   * @param request the {@link Builder request} to authenticate
   * @param headers the current request headers; the map is mutable and can be modified
   * @param <R> the type of the request
   * @param <B> the type of the request builder
   */
  <R extends ClientRequest, B extends ClientRequest.Builder<R, B>> void authenticate(
      ClientRequest.Builder<R, B> request, Map<String, String> headers);
}
