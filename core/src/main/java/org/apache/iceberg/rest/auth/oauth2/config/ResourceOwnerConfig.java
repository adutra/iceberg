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
package org.apache.iceberg.rest.auth.oauth2.config;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.Secret;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Configuration properties for the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.3">Resource Owner Password
 * Credentials Grant</a> flow.
 *
 * <p>Note: according to the <a
 * href="https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics#section-2.4">OAuth
 * 2.0 Security Best Current Practice, section 2.4</a> this flow should NOT be used anymore because
 * it "insecurely exposes the credentials of the resource owner to the client".
 */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface ResourceOwnerConfig {

  String GROUP_NAME = "resource-owner";
  String PREFIX = OAuth2Config.PREFIX + GROUP_NAME + '.';

  String USERNAME = "username";
  String PASSWORD = "password";

  /**
   * Username to use when authenticating against the OAuth2 server. Required if using OAuth2
   * authentication and {@link GrantType#PASSWORD} grant type, ignored otherwise.
   */
  @ConfigOption(USERNAME)
  Optional<String> username();

  /**
   * Password to use when authenticating against the OAuth2 server. Required if using OAuth2
   * authentication and the {@link GrantType#PASSWORD} grant type, ignored otherwise.
   */
  @ConfigOption(PASSWORD)
  @Value.Redacted
  Optional<Secret> password();

  static ImmutableResourceOwnerConfig.Builder fromProperties(Map<String, String> properties) {
    Map<String, String> props = RESTUtil.extractPrefixMap(properties, PREFIX);
    return ImmutableResourceOwnerConfig.builder()
        .username(ConfigUtils.parseOptional(props, USERNAME))
        .password(ConfigUtils.parseOptional(props, PASSWORD, Secret::new));
  }
}
