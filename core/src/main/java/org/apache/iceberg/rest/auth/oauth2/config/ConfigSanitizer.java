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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A component that sanitizes OAuth2 properties received from a REST catalog server. */
public final class ConfigSanitizer {

  public static final Set<String> DENY_LIST =
      Set.of(
          BasicConfig.CLIENT_ID,
          BasicConfig.CLIENT_SECRET,
          ResourceOwnerConfig.USERNAME,
          ResourceOwnerConfig.PASSWORD,
          ClientAssertionConfig.PRIVATE_KEY);

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigSanitizer.class);

  private final BiConsumer<String, String> logConsumer;

  public ConfigSanitizer() {
    this(LOGGER);
  }

  private ConfigSanitizer(Logger logger) {
    this(logger::warn);
  }

  @VisibleForTesting
  ConfigSanitizer(BiConsumer<String, String> logConsumer) {
    this.logConsumer = logConsumer;
  }

  /** Sanitizes table properties received from the server. */
  public Map<String, String> sanitize(Map<String, String> properties) {
    Map<String, String> sanitized = Maps.newHashMap(properties);
    for (Iterator<String> iterator = sanitized.keySet().iterator(); iterator.hasNext(); ) {
      String key = iterator.next();
      if (DENY_LIST.contains(key)) {
        logConsumer.accept(
            "Ignoring property '{}': this property is not allowed to be vended by catalog servers.",
            key);
        iterator.remove();
      }
    }

    return sanitized;
  }
}
