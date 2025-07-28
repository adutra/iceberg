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
package org.apache.iceberg.rest.oauth2.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.oauth2.config.Secret;
import org.apache.iceberg.rest.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.oauth2.test.TestConstants;
import org.junit.jupiter.api.Test;

class TestClientSecretBasicAuthenticator {

  @Test
  void authenticate() {
    ClientSecretBasicAuthenticator authenticator =
        ImmutableClientSecretBasicAuthenticator.builder()
            .clientId(TestConstants.CLIENT_ID1)
            .clientSecret(Secret.of(TestConstants.CLIENT_SECRET1))
            .build();
    assertThat(authenticator.clientId()).isEqualTo(TestConstants.CLIENT_ID1);
    assertThat(authenticator.clientSecret()).isEqualTo(Secret.of(TestConstants.CLIENT_SECRET1));
    Map<String, String> headers = Maps.newHashMap();
    authenticator.authenticate(ClientCredentialsTokenRequest.builder(), headers);
    assertThat(headers)
        .containsEntry("Authorization", "Basic " + TestConstants.CLIENT_CREDENTIALS1_BASE_64);
  }
}
