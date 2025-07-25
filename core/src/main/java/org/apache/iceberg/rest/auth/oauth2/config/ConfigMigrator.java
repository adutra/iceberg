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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component that migrates legacy OAuth2 properties (from {@link OAuth2Properties}) to the new
 * OAuth2 properties (as declared in {@link OAuth2Config}, and logs warnings when legacy properties
 * are detected.
 */
public final class ConfigMigrator {

  /**
   * The default client ID to use when no client ID is provided in the legacy {@link
   * OAuth2Properties#CREDENTIAL} property.
   */
  public static final String DEFAULT_CLIENT_ID = "iceberg";

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMigrator.class);

  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(":").limit(2).trimResults();

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE =
      "Detected legacy OAuth2 property '{}', please use option{} {} instead.";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_NO_CLIENT_ID =
      "The legacy OAuth2 property 'credential' was provided, but it did not contain a client ID; assuming '{}'.";

  private final BiConsumer<String, String[]> logConsumer;

  private final Set<String> warnings = Collections.newSetFromMap(Maps.newConcurrentMap());

  public ConfigMigrator() {
    this(LOGGER);
  }

  public ConfigMigrator(Logger logger) {
    this(logger::warn);
  }

  @VisibleForTesting
  ConfigMigrator(BiConsumer<String, String[]> logConsumer) {
    this.logConsumer = logConsumer;
  }

  /**
   * Migrates legacy Iceberg OAuth2 properties. Returns a copy of the input map containing only the
   * migrated properties; all returned properties start with the {@value OAuth2Config#PREFIX}
   * prefix.
   */
  public Map<String, String> migrate(Map<String, String> properties) {
    Map<String, String> migrated = Maps.newHashMap();
    for (Entry<String, String> entry : properties.entrySet()) {
      switch (entry.getKey()) {
        case OAuth2Properties.CREDENTIAL:
          warn(
              entry.getKey(),
              true,
              BasicConfig.PREFIX + BasicConfig.CLIENT_ID,
              BasicConfig.PREFIX + BasicConfig.CLIENT_SECRET);
          List<String> parts = CREDENTIAL_SPLITTER.splitToList(entry.getValue());
          if (parts.size() == 2) {
            migrated.put(BasicConfig.PREFIX + BasicConfig.CLIENT_ID, parts.get(0));
            migrated.put(BasicConfig.PREFIX + BasicConfig.CLIENT_SECRET, parts.get(1));
          } else {
            if (warnings.add(DEFAULT_CLIENT_ID)) {
              logConsumer.accept(MESSAGE_TEMPLATE_NO_CLIENT_ID, new String[] {DEFAULT_CLIENT_ID});
            }
            migrated.put(BasicConfig.PREFIX + BasicConfig.CLIENT_ID, DEFAULT_CLIENT_ID);
            migrated.put(BasicConfig.PREFIX + BasicConfig.CLIENT_SECRET, parts.get(0));
          }
          break;
        case OAuth2Properties.TOKEN:
          warn(entry.getKey(), BasicConfig.PREFIX + BasicConfig.TOKEN);
          migrated.put(BasicConfig.PREFIX + BasicConfig.TOKEN, entry.getValue());
          break;
        case OAuth2Properties.TOKEN_EXPIRES_IN_MS:
          warn(
              entry.getKey(), TokenRefreshConfig.PREFIX + TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN);
          Duration duration =
              Duration.ofMillis(
                  PropertyUtil.propertyAsLong(
                      properties,
                      OAuth2Properties.TOKEN_EXPIRES_IN_MS,
                      OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT));
          migrated.put(
              TokenRefreshConfig.PREFIX + TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN,
              duration.toString());
          break;
        case OAuth2Properties.TOKEN_REFRESH_ENABLED:
          warn(entry.getKey(), TokenRefreshConfig.PREFIX + TokenRefreshConfig.ENABLED);
          migrated.put(
              TokenRefreshConfig.PREFIX + TokenRefreshConfig.ENABLED,
              String.valueOf(Boolean.parseBoolean(entry.getValue())));
          break;
        case OAuth2Properties.OAUTH2_SERVER_URI:
          warn(
              entry.getKey(),
              false,
              BasicConfig.PREFIX + BasicConfig.ISSUER_URL,
              BasicConfig.PREFIX + BasicConfig.TOKEN_ENDPOINT);
          migrated.put(BasicConfig.PREFIX + BasicConfig.TOKEN_ENDPOINT, entry.getValue());
          break;
        case OAuth2Properties.SCOPE:
          warn(entry.getKey(), BasicConfig.PREFIX + BasicConfig.SCOPE);
          migrated.put(BasicConfig.PREFIX + BasicConfig.SCOPE, entry.getValue());
          break;
        case OAuth2Properties.AUDIENCE:
          warn(entry.getKey(), TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES);
          migrated.put(
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.AUDIENCES, entry.getValue());
          break;
        case OAuth2Properties.RESOURCE:
          warn(entry.getKey(), TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE);
          migrated.put(TokenExchangeConfig.PREFIX + TokenExchangeConfig.RESOURCE, entry.getValue());
          break;
          // Vended token exchange properties
        case OAuth2Properties.ACCESS_TOKEN_TYPE:
        case OAuth2Properties.ID_TOKEN_TYPE:
        case OAuth2Properties.SAML1_TOKEN_TYPE:
        case OAuth2Properties.SAML2_TOKEN_TYPE:
        case OAuth2Properties.JWT_TOKEN_TYPE:
        case OAuth2Properties.REFRESH_TOKEN_TYPE:
          migrated.put(
              BasicConfig.PREFIX + BasicConfig.GRANT_TYPE, GrantType.TOKEN_EXCHANGE.getValue());
          migrated.put(
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN, entry.getValue());
          migrated.put(
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN_TYPE, entry.getKey());
          warn(
              entry.getKey(),
              true,
              BasicConfig.PREFIX + BasicConfig.GRANT_TYPE,
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN,
              TokenExchangeConfig.PREFIX + TokenExchangeConfig.SUBJECT_TOKEN_TYPE);
          break;
        case OAuth2Properties.TOKEN_EXCHANGE_ENABLED:
          boolean useTokenExchangeForTokenRefreshes = Boolean.parseBoolean(entry.getValue());
          if (useTokenExchangeForTokenRefreshes) {
            migrated.put(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.GRANT_TYPE,
                GrantType.TOKEN_EXCHANGE.getValue());
          } else {
            migrated.put(
                TokenRefreshConfig.PREFIX + TokenRefreshConfig.GRANT_TYPE,
                GrantType.REFRESH_TOKEN.getValue());
          }
          warn(entry.getKey(), TokenRefreshConfig.PREFIX + TokenRefreshConfig.GRANT_TYPE);
          break;
        default:
          if (entry.getKey().startsWith(OAuth2Config.PREFIX)) {
            migrated.put(entry.getKey(), entry.getValue());
          }
      }
    }

    return Map.copyOf(migrated);
  }

  private void warn(String icebergOption, String authManagerOption) {
    warn(icebergOption, false, authManagerOption);
  }

  private void warn(String legacyOption, boolean and, String... newOptions) {
    if (warnings.add(legacyOption)) {
      List<String> options = Lists.newArrayList(newOptions);
      String joined =
          options.size() == 1
              ? options.get(0)
              : options.stream().limit(options.size() - 1).collect(Collectors.joining(", "))
                  + (and ? " and " : " or ")
                  + options.get(options.size() - 1);
      logConsumer.accept(
          MESSAGE_TEMPLATE, new String[] {legacyOption, options.size() == 1 ? "" : "s", joined});
    }
  }
}
