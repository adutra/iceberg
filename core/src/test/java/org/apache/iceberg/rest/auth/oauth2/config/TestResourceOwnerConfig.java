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

import static org.apache.iceberg.rest.auth.oauth2.config.ResourceOwnerConfig.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.oauth2.sdk.auth.Secret;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestResourceOwnerConfig {

  @ParameterizedTest
  @MethodSource
  void testFromProperties(Map<String, String> properties, ResourceOwnerConfig expected) {
    ResourceOwnerConfig actual = ResourceOwnerConfig.fromProperties(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testFromProperties() {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableResourceOwnerConfig.builder().build()),
        Arguments.of(
            Map.of(PREFIX + ResourceOwnerConfig.USERNAME, "user1"),
            ImmutableResourceOwnerConfig.builder().username("user1").build()),
        Arguments.of(
            Map.of(PREFIX + ResourceOwnerConfig.PASSWORD, "pass123"),
            ImmutableResourceOwnerConfig.builder().password(new Secret("pass123")).build()));
  }
}
