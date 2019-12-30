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

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.java.CommonOptions;

import java.nio.charset.StandardCharsets;

public class RawJsonTranscoder implements Transcoder {

  public static RawJsonTranscoder INSTANCE = new RawJsonTranscoder();

  private RawJsonTranscoder() { }

  @Override
  public EncodedValue encode(final Object input) {
    if (input instanceof CommonOptions.BuiltCommonOptions || input instanceof CommonOptions) {
      throw InvalidArgumentException.fromMessage("No content provided, cannot " +
        "encode " + input.getClass().getSimpleName() + " as content!");
    }

    if (input instanceof byte[]) {
      return new EncodedValue((byte[]) input, CodecFlags.JSON_COMPAT_FLAGS);
    } else if (input instanceof String) {
      return new EncodedValue(((String) input).getBytes(StandardCharsets.UTF_8), CodecFlags.JSON_COMPAT_FLAGS);
    } else {
      throw InvalidArgumentException.fromMessage("Only byte[] and String types are supported for the RawJsonTranscoder!");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T decode(final Class<T> target, final byte[] input, int flags) {
    if (target.isAssignableFrom(byte[].class)) {
      return (T) input;
    } else if (target.isAssignableFrom(String.class)) {
      return (T) new String(input, StandardCharsets.UTF_8);
    } else {
      throw new DecodingFailureException("RawJsonTranscoder can only decode into either byte[] or String!");
    }
  }

}
