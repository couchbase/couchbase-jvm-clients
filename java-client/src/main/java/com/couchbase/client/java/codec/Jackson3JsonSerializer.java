/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.java.codec;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A serializer backed by a user-provided Jackson 3 {@code JsonMapper}.
 * <p>
 * In order to use this class, you must add Jackson 3 to your class path.
 * <p>
 * Example usage:
 * <pre>
 * var mapper = JsonMapper.builder().build();
 * var serializer = Jackson3JsonSerializer.create(mapper);
 *
 * // Set as default serializer when connecting
 * Cluster cluster = Cluster.connect(
 *   connectionString,
 *   ClusterOptions.clusterOptions(username, password)
 *     .environment(env -> env
 *       .jsonSerializer(serializer)
 *     )
 * );
 * </pre>
 * <b>WARNING:</b> This serializer ignores the {@link com.couchbase.client.java.encryption.annotation.Encrypted}
 * annotation for automatic Field-Level Encryption (FLE). Automatic FLE with data binding requires using
 * {@link JacksonJsonSerializer} and Jackson 2, or the default serializer which uses a repackaged version of Jackson 2.
 */
public class Jackson3JsonSerializer implements JsonSerializer {
  private final JsonMapper mapper;

  /**
   * Returns a new instance backed by the given mapper.
   *
   * @param mapper the custom JsonMapper to use.
   */
  public static Jackson3JsonSerializer create(JsonMapper mapper) {
    return new Jackson3JsonSerializer(mapper);
  }

  private Jackson3JsonSerializer(JsonMapper mapper) {
    this.mapper = requireNonNull(mapper);
  }

  @Override
  public byte[] serialize(final Object input) {
    if (input instanceof byte[]) {
      return (byte[]) input;
    }

    try {
      return mapper.writeValueAsBytes(input);
    } catch (Throwable t) {
      throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", t);
    }
  }

  @Override
  public <T> T deserialize(final Class<T> target, final byte[] input) {
    if (target.equals(byte[].class)) {
      return (T) input;
    }

    try {
      return mapper.readValue(input, target);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
        + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }

  @Override
  public <T> T deserialize(final TypeRef<T> target, final byte[] input) {
    try {
      JavaType type = mapper.getTypeFactory().constructType(target.type());
      return mapper.readValue(input, type);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
        + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }
}
