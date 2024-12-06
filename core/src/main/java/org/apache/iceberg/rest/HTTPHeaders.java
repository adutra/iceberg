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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.immutables.value.Value;

/** Represents a set of HTTP headers. Header name comparison is case-insensitive. */
@Value.Style(depluralize = true)
@Value.Immutable
public interface HTTPHeaders {

  HTTPHeaders EMPTY = ImmutableHTTPHeaders.builder().build();

  @Value.Parameter(order = 0)
  List<HTTPHeader> headers();

  /**
   * Returns a map representation of the headers where each header name is mapped to a list of its
   * values.
   */
  @Value.Lazy
  default Map<String, List<String>> asMap() {
    return headers().stream()
        .collect(Collectors.groupingBy(HTTPHeader::lowerCaseName))
        .values()
        .stream()
        .collect(
            Collectors.toMap(
                headers -> headers.get(0).name(),
                header -> header.stream().map(HTTPHeader::value).collect(Collectors.toList())));
  }

  /**
   * Returns a simple map representation of the headers where each header name is mapped to its
   * first value. If a header has multiple values, only the first value is used.
   */
  @Value.Lazy
  default Map<String, String> asSimpleMap() {
    return headers().stream()
        .collect(Collectors.toMap(HTTPHeader::name, HTTPHeader::value, (h1, h2) -> h1));
  }

  /** Returns a {@link ListMultimap} representation of the headers. */
  @Value.Lazy
  default ListMultimap<String, String> asMultiMap() {
    return headers().stream()
        .collect(
            ImmutableListMultimap.toImmutableListMultimap(HTTPHeader::name, HTTPHeader::value));
  }

  /** Returns all the headers for the given header name. */
  default List<HTTPHeader> headers(String name) {
    return headers().stream()
        .filter(header -> header.name().equalsIgnoreCase(name))
        .collect(Collectors.toList());
  }

  /** Returns whether the headers list contains a header with the given name. */
  default boolean contains(String name) {
    return headers().stream().anyMatch(header -> header.name().equalsIgnoreCase(name));
  }

  /**
   * Adds the given header to the current headers if no header with the same name is already
   * present. Returns a new instance with the added header.
   */
  default HTTPHeaders addIfAbsent(HTTPHeader header) {
    return contains(header.name())
        ? this
        : ImmutableHTTPHeaders.builder().from(this).addHeader(header).build();
  }

  /**
   * Adds the given headers to the current headers if no headers with the same names are already
   * present. Returns a new instance with the added headers.
   */
  default HTTPHeaders addIfAbsent(HTTPHeaders headers) {
    Set<HTTPHeader> newHeaders =
        headers.headers().stream().filter(e -> !contains(e.name())).collect(Collectors.toSet());
    return newHeaders.isEmpty()
        ? this
        : ImmutableHTTPHeaders.builder().from(this).addAllHeaders(newHeaders).build();
  }

  static HTTPHeaders of(HTTPHeader... headers) {
    return ImmutableHTTPHeaders.builder().addHeaders(headers).build();
  }

  static HTTPHeaders fromMap(Map<String, ? extends Iterable<String>> headers) {
    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    headers.forEach(
        (name, values) -> values.forEach(value -> builder.addHeader(HTTPHeader.of(name, value))));
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
    String value();

    @Value.Derived
    default String lowerCaseName() {
      return name().toLowerCase(Locale.ROOT);
    }

    static HTTPHeader of(String name, String value) {
      return ImmutableHTTPHeader.of(name, value);
    }
  }
}
