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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOption;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOptions;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public interface ResourceOwnerPasswordConfig {

  ResourceOwnerPasswordConfig DEFAULT = builder().build();

  /**
   * The OAuth2 username. Only relevant for {@link GrantType#PASSWORD} grant type.
   *
   * @see OAuth2Properties.ResourceOwnerPassword#USERNAME
   */
  Optional<String> username();

  /**
   * The OAuth2 password supplier. Only relevant for {@link GrantType#PASSWORD} grant type. Must be
   * set if a password is required.
   */
  Optional<Secret> password();

  /**
   * Merges the given properties into this {@link ResourceOwnerPasswordConfig} and returns the
   * result.
   */
  default ResourceOwnerPasswordConfig merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Builder builder = builder();
    builder.usernameOption().set(properties, username());
    builder.passwordOption().set(properties, password());
    return builder.build();
  }

  static Builder builder() {
    return ImmutableResourceOwnerPasswordConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(ResourceOwnerPasswordConfig config);

    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      usernameOption().set(properties);
      passwordOption().set(properties);
      return this;
    }

    @CanIgnoreReturnValue
    Builder username(String username);

    @CanIgnoreReturnValue
    default Builder password(String password) {
      return password(Secret.of(password));
    }

    @CanIgnoreReturnValue
    Builder password(Secret password);

    ResourceOwnerPasswordConfig build();

    private ConfigOption<String> usernameOption() {
      return ConfigOptions.simple(OAuth2Properties.ResourceOwnerPassword.USERNAME, this::username);
    }

    private ConfigOption<Secret> passwordOption() {
      return ConfigOptions.simple(
          OAuth2Properties.ResourceOwnerPassword.PASSWORD, this::password, Secret::of);
    }
  }
}
