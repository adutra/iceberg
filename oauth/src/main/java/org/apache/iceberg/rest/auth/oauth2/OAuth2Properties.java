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
package org.apache.iceberg.rest.auth.oauth2;

import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.oauth2.grant.GrantCommonNames;

/** Configuration properties for the {@link OAuth2Manager}. */
public final class OAuth2Properties {

  private OAuth2Properties() {}

  public static final String PREFIX = "rest.auth.oauth2.";

  /**
   * Basic OAuth2 properties. These properties are used to configure the basic OAuth2 options such
   * as the issuer URL, token endpoint, client ID, and client secret.
   */
  public static final class Basic {

    /**
     * OAuth2 issuer URL.
     *
     * <p>The root URL of the Authorization server, which will be used for discovering supported
     * endpoints and their locations. For Keycloak, this is typically the realm URL: {@code
     * https://<keycloak-server>/realms/<realm-name>}.
     *
     * <p>Two "well-known" paths are supported for endpoint discovery: {@code
     * .well-known/openid-configuration} and {@code .well-known/oauth-authorization-server}. The
     * full metadata discovery URL will be constructed by appending these paths to the issuer URL.
     *
     * <p>Either this property or {@link #TOKEN_ENDPOINT} must be set.
     *
     * @see <a
     *     href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata">OpenID
     *     Connect Discovery 1.0</a>
     * @see <a href="https://tools.ietf.org/html/rfc8414#section-5">RFC 8414 Section 5</a>
     */
    public static final String ISSUER_URL = PREFIX + "issuer-url";

    /**
     * URL of the OAuth2 token endpoint. For Keycloak, this is typically {@code
     * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token}.
     *
     * <p>Either this property or {@link #ISSUER_URL} must be set. In case it is not set, the token
     * endpoint will be discovered from the {@link #ISSUER_URL issuer URL}, using the OpenID Connect
     * Discovery metadata published by the issuer.
     */
    public static final String TOKEN_ENDPOINT = PREFIX + "token-endpoint";

    /**
     * The grant type to use when authenticating against the OAuth2 server. Valid values are:
     *
     * <ul>
     *   <li>{@value GrantCommonNames#CLIENT_CREDENTIALS}
     * </ul>
     *
     * Optional, defaults to {@value GrantCommonNames#CLIENT_CREDENTIALS}.
     */
    public static final String GRANT_TYPE = PREFIX + "grant-type";

    /** Client ID to use when authenticating against the OAuth2 server. Required. */
    public static final String CLIENT_ID = PREFIX + "client-id";

    /**
     * The OAuth2 client authentication method to use. Valid values are:
     *
     * <ul>
     *   <li>{@code none}: the client does not authenticate itself at the token endpoint, because it
     *       is a public client with no client secret or other authentication mechanism.
     *   <li>{@code client_secret_basic}: client secret is sent in the HTTP Basic Authorization
     *       header.
     *   <li>{@code client_secret_post}: client secret is sent in the request body as a form
     *       parameter.
     * </ul>
     *
     * The default is {@code client_secret_basic} if the client is private, or {@code none} if the
     * client is public.
     */
    public static final String CLIENT_AUTH = PREFIX + "client-auth";

    /**
     * Client secret to use when authenticating against the OAuth2 server. Required if the client is
     * private and is authenticated using the standard "client-secret" methods. If other
     * authentication methods are used, this property is ignored.
     */
    public static final String CLIENT_SECRET = PREFIX + "client-secret";

    /**
     * Space-separated list of scopes to include in each request to the OAuth2 server. Optional,
     * defaults to empty (no scopes).
     *
     * <p>The scope names will not be validated by the OAuth2 agent; make sure they are valid
     * according to <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">RFC 6749
     * Section 3.3</a>.
     */
    public static final String SCOPE = PREFIX + "scope";

    /**
     * Extra parameters to include in each request to the token endpoint. This is useful for custom
     * parameters that are not covered by the standard OAuth2.0 specification. Optional, defaults to
     * empty.
     *
     * <p>This is a prefix property, and multiple values can be set, each with a different key and
     * value. The values must NOT be URL-encoded. Example:
     *
     * <pre>{@code
     * rest.auth.oauth2.extra-params.custom_param1=custom_value1"
     * rest.auth.oauth2.extra-params.custom_param2=custom_value2"
     * }</pre>
     *
     * For example, Auth0 requires the {@code audience} parameter to be set to the API identifier.
     * This can be done by setting the following configuration:
     *
     * <pre>{@code
     * rest.auth.oauth2.extra-params.audience=https://iceberg-rest-catalog/api
     * }</pre>
     */
    public static final String EXTRA_PARAMS_PREFIX = PREFIX + "extra-params.";

    /**
     * Defines how long the agent should wait for tokens to be acquired. Optional, defaults to 5
     * minutes.
     */
    public static final String TIMEOUT = PREFIX + "timeout";
  }

  /** Configuration properties for the token refresh feature. */
  public static final class TokenRefresh {

    public static final String PREFIX = OAuth2Properties.PREFIX + "token-refresh.";

    /**
     * Whether to enable token refresh. If enabled, the agent will automatically refresh its access
     * token when it expires. If disabled, the agent will only fetch the initial access token, but
     * won't refresh it. Defaults to {@code true}.
     */
    public static final String ENABLED = TokenRefresh.PREFIX + "enabled";

    /**
     * Default access token lifespan; if the OAuth2 server returns an access token without
     * specifying its expiration time, this value will be used. Note that when this happens, a
     * warning will be logged.
     *
     * <p>Optional, defaults to 5 minutes. Must be a valid <a
     * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
     */
    public static final String ACCESS_TOKEN_LIFESPAN =
        TokenRefresh.PREFIX + "access-token-lifespan";

    /**
     * Refresh safety margin to use; a new token will be fetched when the current token's remaining
     * lifespan is less than this value. Optional, defaults to 10 seconds. Must be a valid <a
     * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
     */
    public static final String SAFETY_MARGIN = TokenRefresh.PREFIX + "safety-margin";

    /**
     * Defines for how long the OAuth2 manager should keep the tokens fresh, if the agent is not
     * being actively used. Setting this value too high may cause an excessive usage of network I/O
     * and thread resources; conversely, when setting it too low, if the agent is used again, the
     * calling thread may block if the tokens are expired and need to be renewed synchronously.
     * Optional, defaults to 30 seconds. Must be a valid <a
     * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
     */
    public static final String IDLE_TIMEOUT = TokenRefresh.PREFIX + "idle-timeout";
  }

  /**
   * Configuration properties for the OAuth2 agent runtime.
   *
   * <p>These properties are used to configure the runtime behavior of the OAuth2 agent, such as the
   * agent name.
   */
  @SuppressWarnings("JavaLangClash")
  public static final class Runtime {

    public static final String PREFIX = OAuth2Properties.PREFIX + "runtime.";

    /**
     * The distinctive name of the OAuth2 agent. Defaults to {@code iceberg-auth-manager}. This name
     * is printed in all log messages and user prompts.
     */
    public static final String AGENT_NAME = Runtime.PREFIX + "agent-name";
  }

  /**
   * Configuration properties for the OAuth2 manager.
   *
   * <p>These properties are used to configure the OAuth2 manager, such as the session cache
   * timeout, and whether to migrate legacy Iceberg OAuth2 properties.
   */
  public static final class Manager {

    public static final String PREFIX = OAuth2Properties.PREFIX + "manager.";

    /**
     * The session cache timeout. Cached sessions will become eligible for eviction after this
     * duration of inactivity. Defaults to 1 hour. Must be a valid <a
     * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
     *
     * <p>This value is used for housekeeping; it does not mean that cached sessions will stop
     * working after this time, but that the session cache will evict the session after this time of
     * inactivity. If the context is used again, a new session will be created and cached.
     */
    public static final String SESSION_CACHE_TIMEOUT = Manager.PREFIX + "session-cache-timeout";

    /**
     * Whether to migrate Iceberg OAuth2 legacy properties. Defaults to {@code false}.
     *
     * <p>When enabled, the manager will automatically migrate legacy Iceberg OAuth2 properties to
     * their new equivalents; e.g. it would map {@code oauth2-server-uri} to {@value
     * Basic#TOKEN_ENDPOINT}.
     *
     * <p>When disabled, legacy properties are ignored.
     */
    public static final String MIGRATE_LEGACY_PROPERTIES =
        Manager.PREFIX + "migrate-legacy-properties";
  }
}
