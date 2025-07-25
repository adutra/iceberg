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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigMigrator;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigSanitizer;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuth2Manager implements AuthManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Manager.class);

  private final String name;

  private final ConfigMigrator configMigrator = new ConfigMigrator();
  private final ConfigSanitizer configSanitizer = new ConfigSanitizer();

  private OAuth2Session initSession;
  private OAuth2Session catalogSession;

  private volatile Cache<String, OAuth2Session> bySessionId;
  private volatile Cache<OAuth2Config, OAuth2Session> byConfig;

  private ScheduledExecutorService refreshExecutor;

  public OAuth2Manager(String managerName) {
    this.name = managerName;
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> initProperties) {
    initSession = new OAuth2Session(configMigrator.migrate(initProperties), refreshExecutor());
    return initSession;
  }

  @Override
  public AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> catalogProperties) {
    Map<String, String> migrated = configMigrator.migrate(catalogProperties);
    OAuth2Config catalogConfig = OAuth2Config.fromProperties(migrated);
    // Copy the existing session if the config is the same as the init session
    // to avoid requiring from users to log in again, for human-based flows.
    catalogSession =
        initSession != null && catalogConfig.equals(initSession.config())
            ? initSession.copy()
            : new OAuth2Session(migrated, catalogConfig, refreshExecutor());
    if (initSession != null) {
      initSession.close(); // normally already closed, but just in case
    }
    initSession = null;
    return catalogSession;
  }

  @Override
  public AuthSession contextualSession(SessionContext context, AuthSession parent) {
    if ((context.properties() == null || context.properties().isEmpty())
        && (context.credentials() == null || context.credentials().isEmpty())) {
      return parent;
    }

    Map<String, String> contextProperties =
        RESTUtil.merge(
            Optional.ofNullable(context.properties()).orElseGet(Map::of),
            Optional.ofNullable(context.credentials()).orElseGet(Map::of));
    Map<String, String> migrated = configMigrator.migrate(contextProperties);

    Map<String, String> parentProperties = ((OAuth2Session) parent).properties();
    Map<String, String> childProperties = RESTUtil.merge(parentProperties, migrated);

    OAuth2Config parentConfig = ((OAuth2Session) parent).config();
    OAuth2Config childConfig = OAuth2Config.fromProperties(childProperties);

    if (childConfig.equals(parentConfig)) {
      return parent;
    }

    return bySessionIdCache(childConfig)
        .get(
            context.sessionId(),
            id -> new OAuth2Session(childProperties, childConfig, refreshExecutor()));
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    if (properties.isEmpty()) {
      return parent;
    }

    Map<String, String> migrated = configMigrator.migrate(properties);
    Map<String, String> sanitized = configSanitizer.sanitize(migrated);

    Map<String, String> parentProperties = ((OAuth2Session) parent).properties();
    Map<String, String> childProperties = RESTUtil.merge(parentProperties, sanitized);

    OAuth2Config parentConfig = ((OAuth2Session) parent).config();
    OAuth2Config childConfig = OAuth2Config.fromProperties(childProperties);

    if (childConfig.equals(parentConfig)) {
      return parent;
    }

    return byConfigCache(childConfig)
        .get(childConfig, cfg -> new OAuth2Session(childProperties, cfg, refreshExecutor()));
  }

  @Override
  public AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {

    // Important: this method is invoked from FileIO components, not by the REST catalog.
    // Contrary to the overloaded method tableSession(TableIdentifier, Map, AuthSession),
    // in this method the properties are not sent by the server, so they do not require
    // sanitization.
    Map<String, String> migrated = configMigrator.migrate(properties);

    OAuth2Config config = OAuth2Config.fromProperties(migrated);
    return byConfigCache(config)
        .get(config, cfg -> new OAuth2Session(migrated, cfg, refreshExecutor()));
  }

  @Override
  public void close() {
    OAuth2Session init = initSession;
    OAuth2Session catalog = catalogSession;
    try (catalog;
        init) {
      Cache<String, OAuth2Session> sessionIdCache = bySessionId;
      if (sessionIdCache != null) {
        sessionIdCache.invalidateAll();
        sessionIdCache.cleanUp();
      }
      Cache<OAuth2Config, OAuth2Session> configCache = byConfig;
      if (configCache != null) {
        configCache.invalidateAll();
        configCache.cleanUp();
      }
      ScheduledExecutorService executor = refreshExecutor;
      if (executor != null) { // Iceberg < 1.10 only
        executor.shutdown();
        try {
          if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warn("Timed out waiting for refresh executor to terminate");
            executor.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted while waiting for refresh executor to terminate", e);
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      initSession = null;
      catalogSession = null;
      bySessionId = null;
      byConfig = null;
      refreshExecutor = null;
    }
  }

  private ScheduledExecutorService refreshExecutor() {
    if (refreshExecutor != null) {
      return refreshExecutor;
    }

    try {
      return ThreadPools.authRefreshPool();
    } catch (NoSuchMethodError e) {
      // Iceberg < 1.10 doesn't have ThreadPools.authRefreshPool()
      refreshExecutor = ThreadPools.newScheduledPool(name + "-token-refresh", 1);
      return refreshExecutor;
    }
  }

  private Cache<String, OAuth2Session> bySessionIdCache(OAuth2Config config) {
    Cache<String, OAuth2Session> cache = bySessionId;
    if (cache == null) {
      synchronized (this) {
        if (bySessionId == null) {
          cache = newCache(config);
          bySessionId = cache;
        }
      }
    }

    return cache;
  }

  private Cache<OAuth2Config, OAuth2Session> byConfigCache(OAuth2Config config) {
    Cache<OAuth2Config, OAuth2Session> cache = byConfig;
    if (cache == null) {
      synchronized (this) {
        if (byConfig == null) {
          cache = newCache(config);
          byConfig = cache;
        }
      }
    }

    return cache;
  }

  @VisibleForTesting
  <K> Cache<K, OAuth2Session> newCache(OAuth2Config config) {
    return Caffeine.newBuilder()
        .executor(refreshExecutor())
        .expireAfterAccess(config.basicConfig().sessionCacheTimeout())
        .<K, OAuth2Session>removalListener(
            (id, auth, cause) -> {
              if (auth != null) {
                auth.close();
              }
            })
        .build();
  }
}
