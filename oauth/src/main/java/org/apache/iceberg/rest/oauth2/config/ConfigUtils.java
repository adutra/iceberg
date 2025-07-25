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

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

public final class ConfigUtils {

  private ConfigUtils() {}

  /**
   * Converts a list of scopes into a single string with scopes separated by spaces.
   *
   * @param scopes the list of scopes
   * @return an Optional containing the concatenated string of scopes, or an empty Optional if the
   *     list is empty
   */
  public static Optional<String> scopesAsString(List<String> scopes) {
    return scopes.stream().reduce((a, b) -> a + " " + b);
  }

  /**
   * Converts a string of scopes separated by spaces into a list of individual scopes.
   *
   * @param scopes the string containing scopes separated by spaces
   * @return a list of scopes, or an empty list if the input is null or blank
   */
  public static List<String> scopesAsList(@Nullable String scopes) {
    if (scopes == null || scopes.isBlank()) {
      return List.of();
    }
    return Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(scopes);
  }
}
