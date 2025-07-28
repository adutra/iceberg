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
package org.apache.iceberg.rest.oauth2.grant;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public enum GrantType {
  CLIENT_CREDENTIALS(GrantCanonicalNames.CLIENT_CREDENTIALS, GrantCommonNames.CLIENT_CREDENTIALS);

  private final String canonicalName;
  private final String commonName;

  GrantType(String canonicalName, String commonName) {
    this.canonicalName = canonicalName;
    this.commonName = commonName;
  }

  public String canonicalName() {
    return canonicalName;
  }

  public String commonName() {
    return commonName;
  }

  public static GrantType fromConfigName(String name) {
    Preconditions.checkNotNull(name, "Invalid grant type: null");
    for (GrantType grantType : values()) {
      if (grantType.commonName.equals(name.toLowerCase(Locale.ROOT))
          || grantType.canonicalName.equals(name)) {
        return grantType;
      }
    }

    throw new IllegalArgumentException("Unknown grant type: " + name);
  }

  public boolean initial() {
    return true;
  }
}
