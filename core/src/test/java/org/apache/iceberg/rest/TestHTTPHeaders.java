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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.junit.jupiter.api.Test;

class TestHTTPHeaders {

  final HTTPHeaders headers =
      HTTPHeaders.of(
          HTTPHeader.of("header1", "value1a"),
          HTTPHeader.of("header1", "value1b"),
          HTTPHeader.of("header2", "value2"));

  @Test
  void asMap() {
    assertThat(headers.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2")));
  }

  @Test
  void asSimpleMap() {
    assertThat(headers.asSimpleMap())
        .isEqualTo(
            Map.of(
                "header1", "value1a",
                "header2", "value2"));
  }

  @Test
  void asMultiMap() {
    assertThat(headers.asMultiMap())
        .isEqualTo(
            ImmutableListMultimap.builder()
                .put("header1", "value1a")
                .put("header1", "value1b")
                .put("header2", "value2")
                .build());
  }

  @Test
  void headers() {
    assertThat(headers.headers("header1"))
        .containsExactly(HTTPHeader.of("header1", "value1a"), HTTPHeader.of("header1", "value1b"));
    assertThat(headers.headers("HEADER1"))
        .containsExactly(HTTPHeader.of("header1", "value1a"), HTTPHeader.of("header1", "value1b"));
    assertThat(headers.headers("header2")).containsExactly(HTTPHeader.of("header2", "value2"));
    assertThat(headers.headers("HEADER2")).containsExactly(HTTPHeader.of("header2", "value2"));
    assertThat(headers.headers("header3")).isEmpty();
    assertThat(headers.headers("HEADER3")).isEmpty();
  }

  @Test
  void contains() {
    assertThat(headers.contains("header1")).isTrue();
    assertThat(headers.contains("HEADER1")).isTrue();
    assertThat(headers.contains("header2")).isTrue();
    assertThat(headers.contains("HEADER2")).isTrue();
    assertThat(headers.contains("header3")).isFalse();
    assertThat(headers.contains("HEADER3")).isFalse();
  }

  @Test
  void addIfAbsentHTTPHeader() {
    HTTPHeaders actual = headers.addIfAbsent(HTTPHeader.of("HEADER1", "value1c"));
    assertThat(actual).isSameAs(headers);

    actual = headers.addIfAbsent(HTTPHeader.of("header3", "value3"));
    assertThat(actual.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2"),
                "header3", List.of("value3")));
  }

  @Test
  void addIfAbsentHTTPHeaders() {
    HTTPHeaders actual = headers.addIfAbsent(HTTPHeaders.of(HTTPHeader.of("HEADER1", "value1c")));
    assertThat(actual).isSameAs(headers);

    actual =
        headers.addIfAbsent(
            ImmutableHTTPHeaders.builder()
                .addHeader(HTTPHeader.of("HEADER1", "value1c"))
                .addHeader(HTTPHeader.of("header3", "value3"))
                .build());
    assertThat(actual)
        .isEqualTo(
            ImmutableHTTPHeaders.builder()
                .addHeaders(
                    HTTPHeader.of("header1", "value1a"),
                    HTTPHeader.of("header1", "value1b"),
                    HTTPHeader.of("header2", "value2"),
                    HTTPHeader.of("header3", "value3"))
                .build());
  }

  @Test
  void fromMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromMap(
            ImmutableMap.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2")));
    assertThat(actual).isEqualTo(headers);
  }

  @Test
  void fromSimpleMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromSimpleMap(
            ImmutableMap.of(
                "header1", "value1",
                "header2", "value2"));
    assertThat(actual)
        .isEqualTo(
            ImmutableHTTPHeaders.builder()
                .addHeaders(HTTPHeader.of("header1", "value1"), HTTPHeader.of("header2", "value2"))
                .build());
  }

  @Test
  void fromMultiMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromMultiMap(
            ImmutableListMultimap.<String, String>builder()
                .put("header1", "value1a")
                .put("header2", "value2")
                .put("header1", "value1b")
                .build());
    assertThat(actual).isEqualTo(headers);
  }
}
