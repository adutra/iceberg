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
package org.apache.iceberg.rest.oauth2.rest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;

/**
 * A device authorization request as defined in <a
 * href="https://tools.ietf.org/html/rfc8628#section-3.1">RFC 8628 Section 3.1</a>.
 *
 * <p>This request is used to request an authorization code for a device. The target endpoint is
 * typically the authorization server's device authorization endpoint.
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class DeviceAuthorizationRequest implements RESTRequest, ClientRequest {

  public static final String SCOPE = TokenRequest.SCOPE;

  @Override
  @Check
  public final void validate() {}

  @Nullable
  public abstract String scope();

  @Override
  public final Map<String, String> asFormParameters() {
    Map<String, String> data = Maps.newHashMap(ClientRequest.super.asFormParameters());
    if (scope() != null) {
      data.put(SCOPE, scope());
    }

    return Map.copyOf(data);
  }

  public static Builder builder() {
    return ImmutableDeviceAuthorizationRequest.builder();
  }

  public interface Builder extends ClientRequest.Builder<DeviceAuthorizationRequest, Builder> {

    @CanIgnoreReturnValue
    Builder scope(String scope);
  }
}
