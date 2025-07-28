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
package org.apache.iceberg.rest.auth.oauth2.config.option;

import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

/**
 * A simple configuration option that can be set from a single configuration property.
 *
 * @param <T> the type of the configuration option value
 */
@Value.Immutable
@OAuth2ImmutableStyle
abstract class SimpleConfigOption<T> extends ConfigOption<T> {

  /** The name of the configuration option. */
  protected abstract String option();

  /** An optional converter function to convert the string value to the desired type. */
  protected abstract Function<String, T> converter();

  @Override
  public void set(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    if (properties.containsKey(option())) {
      String value = properties.get(option());
      if (shouldSetOption(value)) {
        setter().accept(converter().apply(value.trim()));
      }
    } else {
      fallback().ifPresent(setter());
    }
  }
}
