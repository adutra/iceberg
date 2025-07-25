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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2">Token Request</a> using
 * the "password" grant type to obtain a new access token.
 *
 * <p>Example:
 *
 * <pre>{@code
 * POST /token HTTP/1.1
 * Host: server.example.com
 * Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=password&username=johndoe&password=A3ddj3w
 * }</pre>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class PasswordTokenRequest implements TokenRequest {

  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";

  @Override
  public final GrantType grantType() {
    return GrantType.PASSWORD;
  }

  /** The resource owner username. */
  public abstract String username();

  /** The resource owner password. */
  public abstract String password();

  @Override
  public final Map<String, String> asFormParameters() {
    Map<String, String> data = Maps.newHashMap(TokenRequest.super.asFormParameters());
    data.put(USERNAME, username());
    data.put(PASSWORD, password());
    return Map.copyOf(data);
  }

  public static Builder builder() {
    return ImmutablePasswordTokenRequest.builder();
  }

  public interface Builder extends TokenRequest.Builder<PasswordTokenRequest, Builder> {
    @CanIgnoreReturnValue
    Builder username(String username);

    @CanIgnoreReturnValue
    Builder password(String password);
  }
}
