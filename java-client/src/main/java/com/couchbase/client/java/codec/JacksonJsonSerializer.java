/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValueModule;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A serializer backed by a user-provided Jackson {@code ObjectMapper}.
 * <p>
 * In order to use this class you must add Jackson to your class path.
 * <p>
 * Make sure to register {@link JsonValueModule} with your {@code ObjectMapper}
 * so it can handle Couchbase {@link JsonObject} instances if desired.
 * <p>
 * Example usage:
 * <pre>
 * ObjectMapper mapper = new ObjectMapper();
 * mapper.registerModule(new JsonValueModule());
 *
 * ClusterEnvironment env = ClusterEnvironment.builder()
 *     .jsonSerializer(new JacksonJsonSerializer(mapper))
 *     .build();
 * </pre>
 * <p>
 *
 * @see JsonValueModule
 */
public class JacksonJsonSerializer implements JsonSerializer {
  private final ObjectMapper mapper;

  /**
   * Returns a new instance backed by a the given ObjectMapper.
   */
  public static JacksonJsonSerializer create(ObjectMapper mapper) {
    return new JacksonJsonSerializer(mapper);
  }

  /**
   * Returns a new instance backed by a default ObjectMapper.
   */
  public static JacksonJsonSerializer create() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JsonValueModule());
    return create(mapper);
  }

  private JacksonJsonSerializer(ObjectMapper mapper) {
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
    if (target.isAssignableFrom(byte[].class)) {
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
