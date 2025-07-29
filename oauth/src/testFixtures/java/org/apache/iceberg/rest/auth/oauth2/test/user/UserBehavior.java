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
package org.apache.iceberg.rest.auth.oauth2.test.user;

import java.util.Optional;
import org.apache.iceberg.rest.auth.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.immutables.value.Value;

/** Describes the desired behavior of a user when interacting with the authorization server. */
@Value.Immutable
@OAuth2ImmutableStyle
public interface UserBehavior {

  /**
   * A simplified user behavior for unit tests, which don't expect a login page but instead expect
   * the authorization server to accept the provided credentials directly.
   */
  UserBehavior UNIT_TESTS = builder().build();

  /**
   * A user behavior for integration tests, which expects a login page and uses the provided
   * credentials to log in.
   */
  UserBehavior INTEGRATION_TESTS =
      builder().username(TestConstants.USERNAME).password(TestConstants.PASSWORD).build();

  static ImmutableUserBehavior.Builder builder() {
    return ImmutableUserBehavior.builder();
  }

  /**
   * An optional username to use when logging in to the authorization server. A username is only
   * required when running integration tests against a real authorization server.
   */
  Optional<String> username();

  /**
   * An optional password to use when logging in to the authorization server. A password is only
   * required when running integration tests against a real authorization server.
   */
  Optional<String> password();

  default String requiredPassword() {
    return password().orElseThrow(() -> new IllegalStateException("Password is required"));
  }

  default String requiredUsername() {
    return username().orElseThrow(() -> new IllegalStateException("Username is required"));
  }

  /**
   * Whether to emulate a user failure, for example by entering a wrong code or by denying consent.
   */
  @Value.Default
  default boolean emulateFailure() {
    return false;
  }
}
