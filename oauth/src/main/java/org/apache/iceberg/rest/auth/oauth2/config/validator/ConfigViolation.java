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
package org.apache.iceberg.rest.auth.oauth2.config.validator;

import com.google.errorprone.annotations.FormatMethod;
import java.util.List;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
interface ConfigViolation {

  @Value.Parameter(order = 1)
  List<String> offendingKeys();

  @Value.Parameter(order = 2)
  String message();

  @Value.Lazy
  default String formattedMessage() {
    return message() + " (" + String.join(" / ", offendingKeys()) + ")";
  }

  static ConfigViolation of(String offendingKey, String message) {
    return of(List.of(offendingKey), "%s", message);
  }

  @FormatMethod
  static ConfigViolation of(String offendingKey, String message, Object... args) {
    return of(offendingKey, String.format(message, args));
  }

  @FormatMethod
  static ConfigViolation of(List<String> offendingKeys, String message, Object... args) {
    return ImmutableConfigViolation.of(offendingKeys, String.format(message, args));
  }

  static ImmutableConfigViolation.Builder builder() {
    return ImmutableConfigViolation.builder();
  }
}
