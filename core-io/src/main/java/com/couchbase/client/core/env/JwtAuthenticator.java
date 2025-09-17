/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import com.couchbase.client.core.io.netty.kv.sasl.OauthBearerSaslClient;
import com.couchbase.client.core.json.Mapper;
import org.jspecify.annotations.NullMarked;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Authenticates with the Couchbase Server cluster using a JSON Web Token (JWT)
 * issued by an Identity Provider.
 * <p>
 * Create a new instance by calling {@link JwtAuthenticator#create(String)}.
 */
@SinceCouchbase("8.1")
@NullMarked
public class JwtAuthenticator implements Authenticator {
  /**
   * @implNote The SASL exchange is handled by {@link OauthBearerSaslClient}
   */
  private static final Set<SaslMechanism> supportedMechanisms =
    unmodifiableSet(EnumSet.of(SaslMechanism.OAUTHBEARER));

  public static JwtAuthenticator create(String jwt) {
    return new JwtAuthenticator(jwt);
  }

  private static class Jwt {
    private final ObjectNode header;
    private final ObjectNode payload;

    private Jwt(ObjectNode header, ObjectNode payload) {
      this.header = requireNonNull(header);
      this.payload = requireNonNull(payload);
    }

    public static Jwt parse(String jwt) {
      String[] parts = jwt.split("\\.");
      if (parts.length != 3) {
        throw new IllegalArgumentException("Expected JWT to have 3 dot-separated components, but found " + parts.length);
      }

      List<byte[]> decodedParts = Arrays.stream(parts)
        .map(Jwt::decodeBas64Url)
        .collect(toList());

      try {
        return new Jwt(
          (ObjectNode) Mapper.decodeIntoTree(decodedParts.get(0)),
          (ObjectNode) Mapper.decodeIntoTree(decodedParts.get(1))
        );
      } catch (Exception e) {
        throw new IllegalArgumentException("Malformed JWT; not JSON", e);
      }
    }

    private static byte[] decodeBas64Url(String s) {
      try {
        return Base64.getUrlDecoder().decode(s);
      } catch (Exception e) {
        throw new IllegalArgumentException("Malformed JWT; component not Base64URL-encoded", e);
      }
    }

    @Override
    public String toString() {
      long expiryEpochSeconds = payload.path("exp").longValue();
      Duration timeUntilExpiry = Duration.ofSeconds(expiryEpochSeconds - Instant.now().getEpochSecond());
      return "Jwt{" +
        "header=" + redactUser(header) +
        ", payload=" + redactUser(payload) +
        ", signature=<redacted>" + // because we don't want a valid JWT to appear in any logs.
        ", timeUntilExpiry=" + timeUntilExpiry +
        '}';
    }
  }

  private final Jwt jwt;
  private final String encodedJwt;
  private final String authHeaderValue;
  private final String username;

  private JwtAuthenticator(String encodedJwt) {
    this.encodedJwt = encodedJwt.trim();
    this.authHeaderValue = "Bearer " + this.encodedJwt;
    this.jwt = Jwt.parse(this.encodedJwt);

    String fieldName = "sub";
    this.username = jwt.payload.path(fieldName).textValue();
    if (this.username == null) {
      throw new IllegalArgumentException("Missing '" + fieldName + "' in JWT payload");
    }
  }

  @Override
  public void authKeyValueConnection(EndpointContext ctx, ChannelPipeline pipeline) {
    pipeline.addLast(new SaslAuthenticationHandler(
      ctx,
      username,
      encodedJwt,
      supportedMechanisms
    ));
  }

  @Override
  public String getAuthHeaderValue() {
    return authHeaderValue;
  }

  @Override
  public String toString() {
    // Omit sensitive properties (like authHeaderValue and encodedJwt)
    // so they don't accidentally end up in a log.
    return "JwtAuthenticator{" +
      "jwt=" + jwt +
      '}';
  }
}
