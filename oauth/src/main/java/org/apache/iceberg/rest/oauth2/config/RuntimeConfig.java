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
import java.io.PrintStream;
import java.time.Clock;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.oauth2.OAuth2Properties;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOption;
import org.apache.iceberg.rest.oauth2.config.option.ConfigOptions;
import org.apache.iceberg.rest.oauth2.config.validator.ConfigValidator;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public interface RuntimeConfig {

  String DEFAULT_AGENT_NAME = "iceberg-auth-manager";

  RuntimeConfig DEFAULT = builder().build();

  /**
   * The distinctive name of the OAuth2 agent. Defaults to {@value #DEFAULT_AGENT_NAME}. This name
   * is printed in all log messages and user prompts.
   *
   * @see OAuth2Properties.Runtime#AGENT_NAME
   */
  @Value.Default
  default String agentName() {
    return DEFAULT_AGENT_NAME;
  }

  /**
   * The clock to use for time-based operations. Defaults to the system clock.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * The {@link PrintStream} to use for console output. Defaults to {@link System#out}.
   *
   * <p>This setting is not exposed as a configuration option and is intended for testing purposes.
   */
  @Value.Default
  @Value.Auxiliary
  default PrintStream console() {
    return System.out;
  }

  @Value.Check
  default void validate() {
    ConfigValidator validator = new ConfigValidator();
    validator.check(
        !agentName().isBlank(),
        OAuth2Properties.Runtime.AGENT_NAME,
        "agent name must not be blank");
    validator.validate();
  }

  /** Merges the given properties into this {@link RuntimeConfig} and returns the result. */
  default RuntimeConfig merge(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Builder builder = builder();
    builder.agentNameOption().set(properties, agentName());
    builder.clock(clock());
    builder.console(console());
    return builder.build();
  }

  static Builder builder() {
    return ImmutableRuntimeConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(RuntimeConfig config);

    @CanIgnoreReturnValue
    default Builder from(Map<String, String> properties) {
      Preconditions.checkNotNull(properties, "Invalid properties map: null");
      agentNameOption().set(properties);
      return this;
    }

    @CanIgnoreReturnValue
    Builder agentName(String agentName);

    @CanIgnoreReturnValue
    Builder clock(Clock clock);

    @CanIgnoreReturnValue
    Builder console(PrintStream console);

    RuntimeConfig build();

    private ConfigOption<String> agentNameOption() {
      return ConfigOptions.simple(OAuth2Properties.Runtime.AGENT_NAME, this::agentName);
    }
  }
}
