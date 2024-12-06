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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableListMultimap;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.junit.jupiter.api.Test;

class TestHTTPHeaders {

  final HTTPHeaders headers =
      ImmutableHTTPHeaders.builder()
          .addHeaders(
              ImmutableHTTPHeader.builder().name("header1").addValues("value1a", "value1b").build(),
              ImmutableHTTPHeader.builder().name("header2").addValues("value2").build())
          .build();

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
  void allHeaderValues() {
    assertThat(headers.allHeaderValues("header1")).containsExactly("value1a", "value1b");
    assertThat(headers.allHeaderValues("header2")).containsExactly("value2");
    assertThat(headers.allHeaderValues("header3")).isEmpty();
  }

  @Test
  void firstHeaderValue() {
    assertThat(headers.firstHeaderValue("header1")).isEqualTo("value1a");
    assertThat(headers.firstHeaderValue("header2")).isEqualTo("value2");
    assertThat(headers.firstHeaderValue("header3")).isNull();
  }

  @Test
  void containsHeader() {
    assertThat(headers.containsHeader("header1")).isTrue();
    assertThat(headers.containsHeader("header2")).isTrue();
    assertThat(headers.containsHeader("header3")).isFalse();
  }

  @Test
  void addHeaderIfAbsent() {
    HTTPHeaders actual = headers.addHeaderIfAbsent("HEADER1", "value1c");
    assertThat(actual).isSameAs(headers);

    actual = headers.addHeaderIfAbsent("header3", "value3");
    assertThat(actual.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2"),
                "header3", List.of("value3")));
  }

  @Test
  void addHeadersIfAbsent() {
    HTTPHeaders actual = headers.addHeadersIfAbsent(HTTPHeaders.of("HEADER1", "value1c"));
    assertThat(actual).isSameAs(headers);

    actual =
        headers.addHeadersIfAbsent(
            ImmutableHTTPHeaders.builder()
                .addHeader(HTTPHeader.of("HEADER1", "value1c"))
                .addHeader(HTTPHeader.of("header3", "value3"))
                .build());
    assertThat(actual.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2"),
                "header3", List.of("value3")));
  }

  @Test
  void addHeadersIfAbsentMap() {
    HTTPHeaders actual = headers.addHeadersIfAbsent(Map.of("HEADER1", "value1c"));
    assertThat(actual).isSameAs(headers);

    HTTPHeaders newHeaders =
        headers.addHeadersIfAbsent(Map.of("HEADER1", "value1c", "header3", "value3"));
    assertThat(newHeaders.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2"),
                "header3", List.of("value3")));
  }

  @Test
  void normalize() {
    HTTPHeaders actual =
        ImmutableHTTPHeaders.builder()
            .addHeader(HTTPHeader.of("header1", "value1a"))
            .addHeader(HTTPHeader.of("HEADER1", "value1b"))
            .addHeader(HTTPHeader.of("header2", "value2"))
            .build();
    assertThat(actual.normalize()).isEqualTo(headers);
  }

  @Test
  void fromMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromMap(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2")));
    assertThat(actual).isEqualTo(headers);
  }

  @Test
  void fromSimpleMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromSimpleMap(
            Map.of(
                "header1", "value1a",
                "header2", "value2"));
    assertThat(actual.headers())
        .containsExactlyInAnyOrder(
            HTTPHeader.of("header1", "value1a"), HTTPHeader.of("header2", "value2"));
  }

  @Test
  void fromMultiMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromMultiMap(
            ImmutableListMultimap.<String, String>builder()
                .put("header1", "value1a")
                .put("header1", "value1b")
                .put("header2", "value2")
                .build());
    assertThat(actual).isEqualTo(headers);
  }

  @Test
  void invalidHeaders() {
    assertThatThrownBy(() -> HTTPHeader.of("header1", List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Header values cannot be empty");
    assertThatThrownBy(() -> HTTPHeader.of("header1", " "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Header value cannot be blank");
  }
}
