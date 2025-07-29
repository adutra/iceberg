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
package org.apache.iceberg.rest.auth.oauth2.rest.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.iceberg.rest.auth.oauth2.rest.ClientCredentialsTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.DefaultTokenResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.MetadataDiscoveryResponse;
import org.apache.iceberg.rest.auth.oauth2.rest.RefreshTokenRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeRequest;
import org.apache.iceberg.rest.auth.oauth2.rest.TokenExchangeResponse;

public final class OAuth2Serializers {

  private OAuth2Serializers() {}

  public static void registerAll(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module
        // Requests
        // ClientCredentialsTokenRequest
        .addSerializer(
            ClientCredentialsTokenRequest.class, new ClientCredentialsTokenRequestSerializer())
        .addDeserializer(
            ClientCredentialsTokenRequest.class, new ClientCredentialsTokenRequestDeserializer())
        // RefreshTokenRequest
        .addSerializer(RefreshTokenRequest.class, new RefreshTokenRequestSerializer())
        .addDeserializer(RefreshTokenRequest.class, new RefreshTokenRequestDeserializer())
        // TokenExchangeRequest
        .addSerializer(TokenExchangeRequest.class, new TokenExchangeRequestSerializer())
        .addDeserializer(TokenExchangeRequest.class, new TokenExchangeRequestDeserializer())
        // Responses
        // DefaultTokenResponse
        .addSerializer(DefaultTokenResponse.class, new DefaultTokenResponseSerializer())
        .addDeserializer(DefaultTokenResponse.class, new DefaultTokenResponseDeserializer())
        // TokenExchangeResponse
        .addSerializer(TokenExchangeResponse.class, new TokenExchangeResponseSerializer())
        .addDeserializer(TokenExchangeResponse.class, new TokenExchangeResponseDeserializer())
        // MetadataDiscoveryResponse
        .addSerializer(MetadataDiscoveryResponse.class, new MetadataDiscoveryResponseSerializer())
        .addDeserializer(
            MetadataDiscoveryResponse.class, new MetadataDiscoveryResponseDeserializer());

    mapper.registerModule(module);
  }

  static class ClientCredentialsTokenRequestSerializer
      extends JsonSerializer<ClientCredentialsTokenRequest> {
    @Override
    public void serialize(
        ClientCredentialsTokenRequest request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      ClientCredentialsTokenRequestParser.toJson(request, gen);
    }
  }

  static class ClientCredentialsTokenRequestDeserializer
      extends JsonDeserializer<ClientCredentialsTokenRequest> {
    @Override
    public ClientCredentialsTokenRequest deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return ClientCredentialsTokenRequestParser.fromJson(jsonNode);
    }
  }

  static class RefreshTokenRequestSerializer extends JsonSerializer<RefreshTokenRequest> {
    @Override
    public void serialize(
        RefreshTokenRequest request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      RefreshTokenRequestParser.toJson(request, gen);
    }
  }

  static class RefreshTokenRequestDeserializer extends JsonDeserializer<RefreshTokenRequest> {
    @Override
    public RefreshTokenRequest deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return RefreshTokenRequestParser.fromJson(jsonNode);
    }
  }

  static class TokenExchangeRequestSerializer extends JsonSerializer<TokenExchangeRequest> {
    @Override
    public void serialize(
        TokenExchangeRequest request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      TokenExchangeRequestParser.toJson(request, gen);
    }
  }

  static class TokenExchangeRequestDeserializer extends JsonDeserializer<TokenExchangeRequest> {
    @Override
    public TokenExchangeRequest deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return TokenExchangeRequestParser.fromJson(jsonNode);
    }
  }

  static class DefaultTokenResponseSerializer extends JsonSerializer<DefaultTokenResponse> {
    @Override
    public void serialize(
        DefaultTokenResponse request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      DefaultTokenResponseParser.toJson(request, gen);
    }
  }

  static class DefaultTokenResponseDeserializer extends JsonDeserializer<DefaultTokenResponse> {
    @Override
    public DefaultTokenResponse deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return DefaultTokenResponseParser.fromJson(jsonNode);
    }
  }

  static class TokenExchangeResponseSerializer extends JsonSerializer<TokenExchangeResponse> {
    @Override
    public void serialize(
        TokenExchangeResponse request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      TokenExchangeResponseParser.toJson(request, gen);
    }
  }

  static class TokenExchangeResponseDeserializer extends JsonDeserializer<TokenExchangeResponse> {
    @Override
    public TokenExchangeResponse deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return TokenExchangeResponseParser.fromJson(jsonNode);
    }
  }

  static class MetadataDiscoveryResponseSerializer
      extends JsonSerializer<MetadataDiscoveryResponse> {
    @Override
    public void serialize(
        MetadataDiscoveryResponse request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      MetadataDiscoveryResponseParser.toJson(request, gen);
    }
  }

  static class MetadataDiscoveryResponseDeserializer
      extends JsonDeserializer<MetadataDiscoveryResponse> {
    @Override
    public MetadataDiscoveryResponse deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return MetadataDiscoveryResponseParser.fromJson(jsonNode);
    }
  }
}
