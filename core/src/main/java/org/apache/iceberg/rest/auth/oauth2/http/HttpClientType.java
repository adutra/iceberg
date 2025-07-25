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
package org.apache.iceberg.rest.auth.oauth2.http;

import com.google.errorprone.annotations.MustBeClosed;
import org.apache.iceberg.rest.auth.oauth2.config.HttpClientConfig;

public enum HttpClientType {
  DEFAULT {
    @Override
    public HttpClient newHttpClient(HttpClientConfig config) {
      return HttpClient.DEFAULT;
    }
  },

  APACHE {
    @Override
    public HttpClient newHttpClient(HttpClientConfig config) {
      return new ApacheHttpClient(config);
    }
  },
  ;

  /** Creates an HTTP client based on the provided configuration. */
  @MustBeClosed
  public abstract HttpClient newHttpClient(HttpClientConfig config);

  public static HttpClientType fromString(String value) {
    for (HttpClientType type : values()) {
      if (type.name().equalsIgnoreCase(value)) {
        return type;
      }
    }

    throw new IllegalArgumentException("Unsupported HTTP client type: " + value);
  }
}
