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

import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.java.CommonOptions;

import static java.util.Objects.requireNonNull;

public class JsonTranscoder implements Transcoder {

  private final JsonSerializer serializer;

  public static JsonTranscoder create(JsonSerializer serializer) {
    return new JsonTranscoder(serializer);
  }

  private JsonTranscoder(final JsonSerializer serializer) {
    this.serializer = requireNonNull(serializer);
  }

  @Override
  public EncodedValue encode(final Object input) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw new IllegalArgumentException("No content provided, cannot " +
        "encode " + input.getClass().getSimpleName() + " as content!");
    }

    if (input instanceof byte[]) {
      throw new IllegalArgumentException("byte[] input is not supported for the JsonTranscoder!. " +
        "If you want to store already encoded JSON, use the RawJsonTranscoder, otherwise store it " +
        "with the RawBinaryTranscoder!");
    }

    return new EncodedValue(serializer.serialize(input), CodecFlags.JSON_COMPAT_FLAGS);
  }

  @Override
  public <T> T decode(final Class<T> target, final byte[] input, int flags) {
    if (target.isAssignableFrom(byte[].class)) {
      throw new IllegalArgumentException("byte[] input is not supported for the JsonTranscoder!. " +
        "If you want to read already encoded JSON, use the RawJsonTranscoder, otherwise read it " +
        "with the RawBinaryTranscoder!");
    }
    return serializer.deserialize(target, input);
  }

  @Override
  public <T> T decode(TypeRef<T> target, byte[] input, int flags) {
    return serializer.deserialize(target, input);
  }

}
