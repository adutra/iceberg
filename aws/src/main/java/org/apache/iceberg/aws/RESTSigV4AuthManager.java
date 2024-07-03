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
package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

public class RESTSigV4AuthManager implements AuthManager {

  private RESTSigV4Signer signer = new RESTSigV4Signer();

  @Override
  public void initialize(String owner, RESTClient client, Map<String, String> properties) {
    signer.initialize(properties);
  }

  @Override
  public AuthSession catalogSession() {
    return signer;
  }

  @Override
  public void close() {
    RESTSigV4Signer restSigV4Signer = signer;
    try {
      if (restSigV4Signer != null) {
        restSigV4Signer.close();
      }
    } finally {
      signer = null;
    }
  }
}
