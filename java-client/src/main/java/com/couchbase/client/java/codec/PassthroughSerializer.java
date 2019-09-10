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

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.error.EncodingFailedException;

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * This serializer acts as a passthrough and expects to deal with already encoded values (likely JSON).
 */
public class PassthroughSerializer implements Serializer {

  public static final PassthroughSerializer INSTANCE = new PassthroughSerializer();

  private PassthroughSerializer() {}

  @Override
  public byte[] serialize(final Object input) {
    try {
      if (input instanceof byte[]) {
        return (byte[]) input;
      }
      return input.toString().getBytes(StandardCharsets.UTF_8);
    } catch (final Throwable t) {
      throw new EncodingFailedException("Handling of serialized JSON content + " + redactUser(input) + " failed.", t);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T deserialize(final Class<T> target, final byte[] input) {
    try {
      if (target.isAssignableFrom(byte[].class)) {
        return (T) input;
      }
      return (T) new String(input, StandardCharsets.UTF_8);
    } catch (final Throwable t) {
      throw new DecodingFailedException("Handling of serialized JSON content into target " + target
        + " failed; encoded = " + redactUser(new String(input, CharsetUtil.UTF_8)), t);
    }
  }

}
