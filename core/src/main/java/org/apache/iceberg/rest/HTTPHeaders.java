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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.immutables.value.Value;

/** Represents a set of HTTP headers. Header name comparison is case-insensitive. */
@Value.Style(depluralize = true)
@Value.Immutable
public interface HTTPHeaders {

  HTTPHeaders EMPTY = ImmutableHTTPHeaders.builder().build();

  @Value.Parameter(order = 0)
  Set<HTTPHeader> headers();

  @Value.Lazy
  default Map<String, List<String>> asMap() {
    return headers().stream().collect(Collectors.toMap(HTTPHeader::name, HTTPHeader::values));
  }

  @Value.Lazy
  default Map<String, String> asSimpleMap() {
    return headers().stream().collect(Collectors.toMap(HTTPHeader::name, HTTPHeader::firstValue));
  }

  @Value.Lazy
  default Multimap<String, String> asMultiMap() {
    return headers().stream()
        .flatMap(header -> header.values().stream().map(value -> Map.entry(header.name(), value)))
        .collect(
            ImmutableListMultimap.toImmutableListMultimap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Returns all the header values for the given header name. */
  default List<String> allHeaderValues(String name) {
    return headers().stream()
        .filter(header -> header.name().equalsIgnoreCase(name))
        .flatMap(header -> header.values().stream())
        .collect(Collectors.toList());
  }

  /**
   * Returns the first header value for the given header name, or null if the header is not present.
   */
  @Nullable
  default String firstHeaderValue(String name) {
    return headers().stream()
        .filter(header -> header.name().equalsIgnoreCase(name))
        .findFirst()
        .map(header -> header.values().get(0))
        .orElse(null);
  }

  /** Returns whether the headers set contains a header with the given name. */
  default boolean containsHeader(String name) {
    return headers().stream().anyMatch(header -> header.name().equalsIgnoreCase(name));
  }

  default HTTPHeaders addHeaderIfAbsent(String name, String value) {
    if (!containsHeader(name)) {
      return ImmutableHTTPHeaders.builder()
          .from(this)
          .addHeader(HTTPHeader.of(name, value))
          .build();
    }

    return this;
  }

  default HTTPHeaders addHeadersIfAbsent(HTTPHeaders headers) {
    Set<HTTPHeader> newHeaders =
        headers.headers().stream()
            .filter(e -> !containsHeader(e.name()))
            .collect(Collectors.toSet());
    if (!newHeaders.isEmpty()) {
      return ImmutableHTTPHeaders.builder().from(this).addAllHeaders(newHeaders).build();
    }

    return this;
  }

  default HTTPHeaders addHeadersIfAbsent(Map<String, String> headers) {
    Set<HTTPHeader> newHeaders =
        headers.entrySet().stream()
            .filter(e -> !containsHeader(e.getKey()))
            .map(e -> HTTPHeader.of(e.getKey(), e.getValue()))
            .collect(Collectors.toSet());
    if (!newHeaders.isEmpty()) {
      return ImmutableHTTPHeaders.builder().from(this).addAllHeaders(newHeaders).build();
    }

    return this;
  }

  @Value.Check
  default HTTPHeaders normalize() {
    if (headers().stream().map(header -> header.name().toLowerCase()).distinct().count()
        != headers().size()) {
      return ImmutableHTTPHeaders.of(
          headers().stream()
              .collect(Collectors.groupingBy(header -> header.name().toLowerCase()))
              .values()
              .stream()
              .map(
                  headers -> {
                    String name = headers.get(0).name();
                    List<String> values =
                        headers.stream()
                            .flatMap(header -> header.values().stream())
                            .collect(Collectors.toList());
                    return HTTPHeader.of(name, values);
                  })
              .collect(Collectors.toSet()));
    }

    return this;
  }

  static HTTPHeaders of(String name, String value) {
    return ImmutableHTTPHeaders.builder().addHeader(HTTPHeader.of(name, value)).build();
  }

  static HTTPHeaders fromMap(Map<String, ? extends Collection<String>> headers) {
    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    headers.forEach((name, values) -> builder.addHeader(HTTPHeader.of(name, List.copyOf(values))));
    return builder.build();
  }

  static HTTPHeaders fromSimpleMap(Map<String, String> headers) {
    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    headers.forEach((name, value) -> builder.addHeader(HTTPHeader.of(name, value)));
    return builder.build();
  }

  static HTTPHeaders fromMultiMap(Multimap<String, String> headers) {
    return fromMap(headers.asMap());
  }

  /** Represents an HTTP header with a name and a list of values. */
  @Value.Style(redactedMask = "****", depluralize = true)
  @Value.Immutable
  interface HTTPHeader {

    @Value.Parameter(order = 0)
    String name();

    @Value.Parameter(order = 1)
    @Value.Redacted
    List<String> values();

    default String firstValue() {
      return values().get(0);
    }

    @Value.Check
    default void check() {
      // While it is technically valid to have a header with no values, we do not allow it
      // as it is not useful in practice and can lead to bugs.
      if (values().isEmpty()) {
        throw new IllegalArgumentException("Header values cannot be empty");
      }
      if (values().stream().anyMatch(String::isBlank)) {
        throw new IllegalArgumentException("Header value cannot be blank");
      }
    }

    static HTTPHeader of(String name, String value) {
      return of(name, List.of(value));
    }

    static HTTPHeader of(String name, List<String> values) {
      return ImmutableHTTPHeader.of(name, values);
    }
  }
}
