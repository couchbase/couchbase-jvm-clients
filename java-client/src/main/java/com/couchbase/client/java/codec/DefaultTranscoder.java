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

import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.error.EncodingFailedException;
import com.couchbase.client.java.CommonOptions;

import java.nio.charset.StandardCharsets;

/**
 * The default implementation of the {@link Transcoder} interface.
 *
 * <p>This transcoder is going to be used for all KV operations if not overridden through the operation options.</p>
 */
public class DefaultTranscoder implements Transcoder {

  private static final JsonSerializer JSON_SERIALIZER = JsonSerializer.INSTANCE;
  private static final JavaObjectSerializer JAVA_OBJECT_SERIALIZER = JavaObjectSerializer.INSTANCE;

  public static final DefaultTranscoder INSTANCE = new DefaultTranscoder();

  private DefaultTranscoder() {}

  @Override
  public byte[] encode(final Object input, final DataFormat format) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw new IllegalArgumentException("No content provided, cannot " +
        "encode " + input.getClass().getSimpleName() + " as content!");
    }

    switch (format) {
      case JSON:
        return JSON_SERIALIZER.serialize(input);
      case ENCODED_JSON:
      case STRING:
        if (input instanceof byte[]) {
          return (byte[]) input;
        }
        return input.toString().getBytes(StandardCharsets.UTF_8);
      case BINARY:
        if (input instanceof byte[]) {
          return (byte[]) input;
        } else {
          throw new EncodingFailedException("BINARY DataFormat needs to be a byte[] as value");
        }
      case OBJECT_SERIALIZATION:
        return JAVA_OBJECT_SERIALIZER.serialize(input);
      default:
        throw new UnsupportedOperationException("DateFormat " + format + " unsupported on encoding");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T decode(final Class<T> target, final byte[] input, final DataFormat format) {
    switch (format) {
      case JSON:
        return JSON_SERIALIZER.deserialize(target, input);
      case ENCODED_JSON:
      case STRING:
        if (target.isAssignableFrom(byte[].class)) {
          return (T) input;
        }
        return (T) new String(input, StandardCharsets.UTF_8);
      case BINARY:
        if (target.isAssignableFrom(byte[].class)) {
          return (T) input;
        } else {
          throw new DecodingFailedException("BINARY DataFormat needs to be a byte[] as target");
        }
      case OBJECT_SERIALIZATION:
        return JAVA_OBJECT_SERIALIZER.deserialize(target, input);
      default:
        throw new UnsupportedOperationException("DateFormat " + format + " unsupported on decoding");
    }
  }

}
