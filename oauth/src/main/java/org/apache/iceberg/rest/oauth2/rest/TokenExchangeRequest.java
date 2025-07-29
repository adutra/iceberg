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
import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Redacted;

/**
 * A <a href="https://datatracker.ietf.org/doc/html/rfc8693/#section-2.1">Token Exchange Request</a>
 * that is used to exchange an access token for a pair of access + refresh tokens.
 *
 * <p>Example:
 *
 * <pre>{@code
 * POST /as/token.oauth2 HTTP/1.1
 * Host: as.example.com
 * Authorization: Basic cnMwODpsb25nLXNlY3VyZS1yYW5kb20tc2VjcmV0
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange
 * &resource=https%3A%2F%2Fbackend.example.com%2Fapi
 * &subject_token=accVkjcJyb4BWCxGsndESCJQbdFMogUC5PbRDqceLTC
 * &subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token
 * }</pre>
 */
@Value.Immutable
@OAuth2ImmutableStyle
public abstract class TokenExchangeRequest implements TokenRequest {

  public static final String RESOURCE = "resource";
  public static final String AUDIENCE = "audience";
  public static final String REQUESTED_TOKEN_TYPE = "requested_token_type";
  public static final String SUBJECT_TOKEN = "subject_token";
  public static final String SUBJECT_TOKEN_TYPE = "subject_token_type";
  public static final String ACTOR_TOKEN = "actor_token";
  public static final String ACTOR_TOKEN_TYPE = "actor_token_type";

  @Override
  public final GrantType grantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  /**
   * OPTIONAL. A URI that indicates the target service or resource where the client intends to use
   * the requested security token. This enables the authorization server to apply policy as
   * appropriate for the target, such as determining the type and content of the token to be issued
   * or if and how the token is to be encrypted.
   */
  @Nullable
  public abstract URI resource();

  /**
   * OPTIONAL. The logical name of the target service where the client intends to use the requested
   * security token. This serves a purpose similar to the resource parameter but with the client
   * providing a logical name for the target service. Interpretation of the name requires that the
   * value be something that both the client and the authorization server understand.
   */
  @Nullable
  public abstract String audience();

  /**
   * OPTIONAL. An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, for the type of
   * the requested security token. If the requested type is unspecified, the issued token type is at
   * the discretion of the authorization server and may be dictated by knowledge of the requirements
   * of the service or resource indicated by the resource or audience parameter.
   */
  @Nullable
  public abstract URI requestedTokenType();

  /**
   * A security token that represents the identity of the party on behalf of whom the request is
   * being made. Typically, the subject of this token will be the subject of the security token
   * issued in response to the request.
   */
  @Redacted
  @SuppressWarnings("SafeLoggingPropagation")
  public abstract String subjectToken();

  /**
   * An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, that indicates
   * the type of the security token in the subject_token parameter.
   */
  public abstract URI subjectTokenType();

  /**
   * OPTIONAL. A security token that represents the identity of the acting party. Typically, this
   * will be the party that is authorized to use the requested security token and act on behalf of
   * the subject.
   */
  @Nullable
  @Redacted
  @SuppressWarnings("SafeLoggingPropagation")
  public abstract String actorToken();

  /**
   * An identifier, as described in <a
   * href="https://datatracker.ietf.org/doc/html/rfc8693/#section-3">Section 3</a>, that indicates
   * the type of the security token in the actor_token parameter. This is REQUIRED when the
   * actor_token parameter is present in the request but MUST NOT be included otherwise.
   */
  @Nullable
  public abstract URI actorTokenType();

  @Override
  public final Map<String, String> asFormParameters() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.putAll(TokenRequest.super.asFormParameters());

    URI resource = resource();
    if (resource != null) {
      builder.put(RESOURCE, resource.toString());
    }

    String audience = audience();
    if (audience != null) {
      builder.put(AUDIENCE, audience);
    }

    URI requestedTokenType = requestedTokenType();
    if (requestedTokenType != null) {
      builder.put(REQUESTED_TOKEN_TYPE, requestedTokenType.toString());
    }

    builder.put(SUBJECT_TOKEN, subjectToken());
    builder.put(SUBJECT_TOKEN_TYPE, subjectTokenType().toString());

    String actorToken = actorToken();
    if (actorToken != null) {
      builder.put(ACTOR_TOKEN, actorToken);
    }

    URI actorTokenType = actorTokenType();
    if (actorTokenType != null) {
      builder.put(ACTOR_TOKEN_TYPE, actorTokenType.toString());
    }

    return builder.buildKeepingLast();
  }

  public static Builder builder() {
    return ImmutableTokenExchangeRequest.builder();
  }

  public interface Builder
      extends TokenRequest.Builder<TokenExchangeRequest, Builder>,
          ClientRequest.Builder<TokenExchangeRequest, Builder> {

    @CanIgnoreReturnValue
    Builder resource(URI resource);

    @CanIgnoreReturnValue
    Builder audience(String audience);

    @CanIgnoreReturnValue
    Builder requestedTokenType(URI requestedTokenType);

    @CanIgnoreReturnValue
    Builder subjectToken(String subjectToken);

    @CanIgnoreReturnValue
    Builder subjectTokenType(URI subjectTokenType);

    @CanIgnoreReturnValue
    Builder actorToken(String actorToken);

    @CanIgnoreReturnValue
    Builder actorTokenType(URI actorTokenType);
  }
}
