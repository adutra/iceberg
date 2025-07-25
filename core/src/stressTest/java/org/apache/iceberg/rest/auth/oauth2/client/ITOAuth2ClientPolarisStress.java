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
package org.apache.iceberg.rest.auth.oauth2.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.PolarisExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PolarisExtension.class)
public class ITOAuth2ClientPolarisStress extends ITOAuth2ClientStressBase {

  @Test
  void backgroundRefreshAndSleep(
      ImmutableTestEnvironment.Builder envBuilder1, ImmutableTestEnvironment.Builder envBuilder2)
      throws ExecutionException, InterruptedException {
    run(envBuilder1, envBuilder2);
  }

  @Override
  protected void authenticate(OAuth2Client client) {
    AccessToken accessToken = client.authenticate();
    try {
      JWT jwt = JWTParser.parse(accessToken.getValue());
      assertThat(jwt).isNotNull();
      assertThat(jwt.getJWTClaimsSet().getBooleanClaim("active")).isTrue();
      assertThat(jwt.getJWTClaimsSet().getStringClaim("scope")).isEqualTo("PRINCIPAL_ROLE:ALL");
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
}
