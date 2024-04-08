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

package com.couchbase.columnar.client.java.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JavaType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.columnar.client.java.DataConversionException;
import com.couchbase.columnar.client.java.codec.Deserializer;
import com.couchbase.columnar.client.java.codec.JacksonDeserializer;
import com.couchbase.columnar.client.java.codec.TypeRef;

import java.io.IOException;

/**
 * For internal SDK use only.
 *
 * @implNote The serializer is backed by a repackaged version of Jackson,
 * but this is an implementation detail users should not depend on.
 * <p>
 * Be aware that this serializer does not recognize standard Jackson annotations.
 * @see JacksonDeserializer
 */
@Stability.Internal
public class InternalJacksonSerDes implements JsonSerializer, Deserializer {

  public static final InternalJacksonSerDes INSTANCE = new InternalJacksonSerDes();

  private final ObjectMapper mapper = Mapper.newObjectMapper();

  private InternalJacksonSerDes() {
    mapper.registerModule(new RepackagedJsonValueModule());
  }

  @Override
  public byte[] serialize(final Object input) {
    try {
      return mapper.writeValueAsBytes(input);
    } catch (Exception e) {
      throw new DataConversionException(e);
    }
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
}
