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
package org.apache.iceberg.rest.oauth2.config;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Represents the OAuth2 dialects supported by the Iceberg OAuth2 Auth Manager. */
public enum Dialect {

  /** Standard OAuth2 dialect, compliant with RFC 6749 and subsequent specs. */
  STANDARD,

  /**
   * Iceberg-specific OAuth2 dialect, used when the catalog server is the authorization server and
   * exposes a token endpoint.
   */
  ICEBERG_REST,
  ;

  public static Dialect fromConfigName(String name) {
    Preconditions.checkNotNull(name, "Invalid OAuth2 dialect: null");
    try {
      return valueOf(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException ignore) {
      throw new IllegalArgumentException("Unknown OAuth2 dialect: " + name);
    }
  }
}
