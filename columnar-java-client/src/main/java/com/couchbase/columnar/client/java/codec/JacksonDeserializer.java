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

package com.couchbase.columnar.client.java.codec;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.columnar.client.java.ClusterOptions;
import com.couchbase.columnar.client.java.QueryOptions;
import com.couchbase.columnar.client.java.internal.ThreadSafe;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A deserializer backed by <a href="https://github.com/FasterXML/jackson">Jackson</a>.
 * <p>
 * Example usage:
 * <pre>
 * var mapper = JsonMapper.builder()
 *     .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
 *     .build();
 * var deserializer = new JacksonDeserializer(mapper);
 * </pre>
 *
 * @see ClusterOptions#deserializer(Deserializer)
 * @see QueryOptions#deserializer(Deserializer)
 */
@ThreadSafe
public final class JacksonDeserializer implements Deserializer {
  private final ObjectMapper mapper;

  public JacksonDeserializer(ObjectMapper mapper) {
    this.mapper = requireNonNull(mapper);
  }

  @Override
  public <T> T deserialize(final Class<T> target, final byte[] input) throws IOException {
    return mapper.readValue(input, target);
  }

  @Override
  public <T> T deserialize(final TypeRef<T> target, final byte[] input) throws IOException {
    JavaType type = mapper.getTypeFactory().constructType(target.type());
    return mapper.readValue(input, type);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
