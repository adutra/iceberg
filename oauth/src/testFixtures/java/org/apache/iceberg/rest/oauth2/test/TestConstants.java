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
package org.apache.iceberg.rest.oauth2.test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

public final class TestConstants {

  private TestConstants() {}

  public static final String CLIENT_ID1 = "Client1";

  public static final String CLIENT_SECRET1 = "s3cr3t";

  public static final String SCOPE1 = "catalog";

  public static final String CLIENT_CREDENTIALS1_BASE_64 =
      Base64.getEncoder()
          .encodeToString((CLIENT_ID1 + ":" + CLIENT_SECRET1).getBytes(StandardCharsets.UTF_8));

  public static final Instant NOW = Instant.parse("2025-01-01T00:00:00Z");

  public static final int ACCESS_TOKEN_EXPIRES_IN_SECONDS = 3600;
  public static final Instant ACCESS_TOKEN_EXPIRATION_TIME =
      NOW.plusSeconds(ACCESS_TOKEN_EXPIRES_IN_SECONDS);
  public static final Duration ACCESS_TOKEN_LIFESPAN =
      Duration.ofSeconds(ACCESS_TOKEN_EXPIRES_IN_SECONDS);

  public static final int REFRESH_TOKEN_EXPIRES_IN_SECONDS = 86400;
  public static final Instant REFRESH_TOKEN_EXPIRATION_TIME =
      NOW.plusSeconds(REFRESH_TOKEN_EXPIRES_IN_SECONDS);
  public static final Duration REFRESH_TOKEN_LIFESPAN =
      Duration.ofSeconds(REFRESH_TOKEN_EXPIRES_IN_SECONDS);
}
