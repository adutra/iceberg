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
package org.apache.iceberg.rest.auth.oauth2.compat;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Properties;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component that migrates legacy Iceberg Core OAuth2 properties to the new OAuth2 properties, and
 * logs warnings when legacy properties are detected.
 */
public final class LegacyPropertiesMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyPropertiesMigrator.class);

  private final BiConsumer<String, String[]> logConsumer;

  private final Set<String> warnedProperties = Collections.newSetFromMap(Maps.newConcurrentMap());

  public LegacyPropertiesMigrator() {
    this(LOGGER);
  }

  public LegacyPropertiesMigrator(Logger logger) {
    this(logger::warn);
  }

  @VisibleForTesting
  LegacyPropertiesMigrator(BiConsumer<String, String[]> logConsumer) {
    this.logConsumer = logConsumer;
  }

  /**
   * Migrates legacy Iceberg OAuth2 properties. Returns a copy of the input map containing only the
   * migrated properties; all returned properties start with the {@value OAuth2Properties#PREFIX}
   * prefix.
   */
  public Map<String, String> migrate(Map<String, String> properties) {
    Map<String, String> migrated = Maps.newHashMap();
    for (Entry<String, String> entry : properties.entrySet()) {
      switch (entry.getKey()) {
        case org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL:
          warnOnLegacyIcebergOAuth2Property(
              entry.getKey(),
              OAuth2Properties.Basic.CLIENT_ID,
              OAuth2Properties.Basic.CLIENT_SECRET,
              true);
          List<String> parts = Splitter.on(':').splitToList(entry.getValue());
          switch (parts.size()) {
            case 2:
              migrated.put(OAuth2Properties.Basic.CLIENT_ID, parts.get(0));
              migrated.put(OAuth2Properties.Basic.CLIENT_SECRET, parts.get(1));
              break;
            case 1:
              // Iceberg dialect: client secret without client id
              migrated.put(OAuth2Properties.Basic.CLIENT_SECRET, parts.get(0));
              break;
            default:
              throw new IllegalArgumentException("Invalid credential: " + entry.getValue());
          }

          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_EXPIRES_IN_MS:
          warnOnLegacyIcebergOAuth2Property(
              entry.getKey(), OAuth2Properties.TokenRefresh.ACCESS_TOKEN_LIFESPAN);
          Duration duration =
              Duration.ofMillis(
                  PropertyUtil.propertyAsLong(
                      properties,
                      org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_EXPIRES_IN_MS,
                      org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT));
          migrated.put(OAuth2Properties.TokenRefresh.ACCESS_TOKEN_LIFESPAN, duration.toString());
          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_REFRESH_ENABLED:
          warnOnLegacyIcebergOAuth2Property(entry.getKey(), OAuth2Properties.TokenRefresh.ENABLED);
          migrated.put(
              OAuth2Properties.TokenRefresh.ENABLED,
              String.valueOf(Boolean.parseBoolean(entry.getValue())));
          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.OAUTH2_SERVER_URI:
          warnOnLegacyIcebergOAuth2Property(
              entry.getKey(),
              OAuth2Properties.Basic.ISSUER_URL,
              OAuth2Properties.Basic.TOKEN_ENDPOINT,
              false);
          migrated.put(OAuth2Properties.Basic.TOKEN_ENDPOINT, entry.getValue());
          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.SCOPE:
          warnOnLegacyIcebergOAuth2Property(entry.getKey(), OAuth2Properties.Basic.SCOPE);
          migrated.put(OAuth2Properties.Basic.SCOPE, entry.getValue());
          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.AUDIENCE:
          warnOnLegacyIcebergOAuth2Property(
              entry.getKey(), OAuth2Properties.TokenExchange.AUDIENCE);
          migrated.put(OAuth2Properties.TokenExchange.AUDIENCE, entry.getValue());
          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.RESOURCE:
          warnOnLegacyIcebergOAuth2Property(
              entry.getKey(), OAuth2Properties.TokenExchange.RESOURCE);
          migrated.put(OAuth2Properties.TokenExchange.RESOURCE, entry.getValue());
          break;
        case org.apache.iceberg.rest.auth.OAuth2Properties.ACCESS_TOKEN_TYPE:
        case org.apache.iceberg.rest.auth.OAuth2Properties.ID_TOKEN_TYPE:
        case org.apache.iceberg.rest.auth.OAuth2Properties.SAML1_TOKEN_TYPE:
        case org.apache.iceberg.rest.auth.OAuth2Properties.SAML2_TOKEN_TYPE:
        case org.apache.iceberg.rest.auth.OAuth2Properties.JWT_TOKEN_TYPE:
          warnOnIgnoredIcebergOauth2Property(
              entry.getKey(), "vended token exchange is not supported");
          break;
        default:
          if (entry.getKey().startsWith(OAuth2Properties.PREFIX)) {
            migrated.put(entry.getKey(), entry.getValue());
          }
      }
    }

    return Map.copyOf(migrated);
  }

  private void warnOnLegacyIcebergOAuth2Property(String icebergOption, String authManagerOption) {
    if (warnedProperties.add(icebergOption)) {
      logConsumer.accept(
          "Detected legacy property '{}', please use option {} instead.",
          new String[] {icebergOption, authManagerOption});
    }
  }

  private void warnOnLegacyIcebergOAuth2Property(
      String icebergOption, String authManagerOption1, String authManagerOption2, boolean and) {
    if (warnedProperties.add(icebergOption)) {
      logConsumer.accept(
          "Detected legacy property '{}', please use options {} {} {} instead.",
          new String[] {icebergOption, authManagerOption1, and ? "and" : "or", authManagerOption2});
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void warnOnIgnoredIcebergOauth2Property(String icebergOption, String reason) {
    if (warnedProperties.add(icebergOption)) {
      logConsumer.accept(
          "Ignoring legacy property '{}': {}.", new String[] {icebergOption, reason});
    }
  }
}
