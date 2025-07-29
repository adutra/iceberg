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
package org.apache.iceberg.rest.oauth2.endpoint;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.oauth2.flow.FlowErrorHandler;
import org.apache.iceberg.rest.oauth2.grant.GrantType;
import org.apache.iceberg.rest.oauth2.immutables.OAuth2ImmutableStyle;
import org.apache.iceberg.rest.oauth2.rest.MetadataDiscoveryResponse;
import org.immutables.value.Value;

@Value.Immutable
@OAuth2ImmutableStyle
public abstract class EndpointProvider {

  /**
   * Common locations for OpenID provider metadata.
   *
   * @see <a
   *     href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata">OpenID
   *     Connect Discovery 1.0</a>
   * @see <a href="https://tools.ietf.org/html/rfc8414#section-5">RFC 8414 Section 5</a>
   */
  public static final List<String> WELL_KNOWN_PATHS =
      List.of(".well-known/openid-configuration", ".well-known/oauth-authorization-server");

  public static Builder builder() {
    return ImmutableEndpointProvider.builder();
  }

  /**
   * The issuer URL as provided in the configuration. Either this or the token endpoint must be
   * configured for the endpoint provider to work.
   */
  protected abstract Optional<URI> issuerUrl();

  /**
   * The token endpoint as provided in the configuration. Either this or the issuer URL must be
   * configured for the endpoint provider to work.
   */
  protected abstract Optional<URI> tokenEndpoint();

  /**
   * The authorization endpoint as provided in the configuration. Only used when the grant type is
   * {@link GrantType#AUTHORIZATION_CODE}. Either this or the issuer URL must be configured for the
   * endpoint provider to work.
   */
  protected abstract Optional<URI> authorizationEndpoint();

  /**
   * The device authorization endpoint as provided in the configuration. Only used when the grant
   * type is {@link GrantType#DEVICE_CODE}. Either this or the issuer URL must be configured for the
   * endpoint provider to work.
   */
  protected abstract Optional<URI> deviceAuthorizationEndpoint();

  protected abstract Supplier<RESTClient> restClientSupplier();

  @Value.Lazy
  public URI resolvedTokenEndpoint() {
    return tokenEndpoint().orElseGet(() -> openIdProviderMetadata().tokenEndpoint());
  }

  @Value.Lazy
  public URI resolvedAuthorizationEndpoint() {
    return authorizationEndpoint()
        .orElseGet(() -> openIdProviderMetadata().authorizationEndpoint());
  }

  @Value.Lazy
  public URI resolvedDeviceAuthorizationEndpoint() {
    return deviceAuthorizationEndpoint()
        .or(() -> Optional.ofNullable(openIdProviderMetadata().deviceAuthorizationEndpoint()))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "OpenID provider metadata does not contain a device authorization endpoint"));
  }

  @Value.Lazy
  protected MetadataDiscoveryResponse openIdProviderMetadata() {
    URI issuerUrl =
        issuerUrl().orElseThrow(() -> new IllegalStateException("No issuer URL configured"));
    return fetchOpenIdProviderMetadata(issuerUrl);
  }

  private MetadataDiscoveryResponse fetchOpenIdProviderMetadata(URI issuerUrl) {
    List<Exception> failures = null;
    for (String path : WELL_KNOWN_PATHS) {
      try {
        URI uri = new URIBuilder(issuerUrl).appendPath(path).build().normalize();
        return restClientSupplier()
            .get()
            .get(
                uri.toString(),
                MetadataDiscoveryResponse.class,
                Map.of("Accept", "application/json"),
                FlowErrorHandler.INSTANCE);
      } catch (Exception e) {
        if (failures == null) {
          failures = Lists.newArrayListWithCapacity(WELL_KNOWN_PATHS.size());
        }

        failures.add(e);
      }
    }

    Preconditions.checkState(failures != null, "collected failures should not be null");
    RESTException toThrow =
        new RESTException(failures.get(0), "Failed to fetch OpenID provider metadata");
    for (int i = 1; i < failures.size(); i++) {
      toThrow.addSuppressed(failures.get(i));
    }

    throw toThrow;
  }

  public interface Builder {

    @CanIgnoreReturnValue
    Builder from(EndpointProvider endpointProvider);

    @CanIgnoreReturnValue
    Builder issuerUrl(URI issuerUrl);

    @CanIgnoreReturnValue
    Builder tokenEndpoint(URI tokenEndpoint);

    @CanIgnoreReturnValue
    Builder authorizationEndpoint(URI authorizationEndpoint);

    @CanIgnoreReturnValue
    Builder deviceAuthorizationEndpoint(URI deviceAuthorizationEndpoint);

    @CanIgnoreReturnValue
    Builder restClientSupplier(Supplier<RESTClient> restClient);

    EndpointProvider build();
  }
}
