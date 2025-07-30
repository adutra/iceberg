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
package org.apache.iceberg.rest.auth.oauth2.agent;

import static org.apache.iceberg.rest.auth.oauth2.grant.GrantType.AUTHORIZATION_CODE;
import static org.apache.iceberg.rest.auth.oauth2.grant.GrantType.TOKEN_EXCHANGE;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestConstants;
import org.apache.iceberg.rest.auth.oauth2.test.container.KeycloakTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.token.AccessToken;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(KeycloakTestEnvironment.class)
public class ITOAuth2AgentLongKeycloak extends ITOAuth2AgentLongBase {

  @Test
  void backgroundRefreshAndSleep(
      ImmutableTestEnvironment.Builder envBuilder1, ImmutableTestEnvironment.Builder envBuilder2)
      throws ExecutionException, InterruptedException {
    run(envBuilder1, envBuilder2.grantType(TOKEN_EXCHANGE).subjectGrantType(AUTHORIZATION_CODE));
  }

  @Override
  protected void authenticate(OAuth2Agent agent) {
    AccessToken accessToken = agent.authenticate();
    DecodedJWT jwt = JWT.decode(accessToken.payload());
    assertThat(jwt).isNotNull();
    assertThat(jwt.getClaim("azp").asString()).isEqualTo(TestConstants.CLIENT_ID1);
    assertThat(jwt.getClaim("scope").asString()).contains(TestConstants.SCOPE1);
  }
}
