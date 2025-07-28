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
package org.apache.iceberg.rest.oauth2.flow;

import static org.apache.iceberg.rest.oauth2.test.TokenAssertions.assertTokens;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.oauth2.token.Tokens;
import org.junit.jupiter.api.Test;

class TestClientCredentialsFlow {

  @Test
  void fetchNewTokens() throws InterruptedException, ExecutionException {
    try (TestEnvironment env = TestEnvironment.builder().build();
        FlowFactory flowFactory = env.createFlowFactory()) {
      InitialFlow flow = flowFactory.createInitialFlow();
      assertThat(flow).isInstanceOf(ClientCredentialsFlow.class);
      Tokens tokens = flow.fetchNewTokens().toCompletableFuture().get();
      assertTokens(tokens, "access_initial", "refresh_initial");
    }
  }
}
