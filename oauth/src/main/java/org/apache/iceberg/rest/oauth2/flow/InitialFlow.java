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

import java.util.concurrent.CompletionStage;
import org.apache.iceberg.rest.oauth2.token.Tokens;

/** An interface representing an "initial" OAuth2 flow that can be used to fetch new tokens. */
public interface InitialFlow extends Flow {

  /**
   * Fetches brand-new tokens from the OAuth2 provider.
   *
   * <p>This method is called when new tokens are needed, either because no current tokens exist, or
   * because the current tokens are expired or about to expire.
   *
   * <p>A flow may be stateful or stateless. Stateful flows should clean up internal resources when
   * the returned {@link CompletionStage} completes.
   *
   * @return A stage that completes when brand-new tokens are fetched.
   */
  CompletionStage<Tokens> fetchNewTokens();
}
