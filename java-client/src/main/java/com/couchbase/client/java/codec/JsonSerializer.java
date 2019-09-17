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
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JacksonTransformers;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * The default JSON serializer based on jackson.
 *
 * <p>This serializer uses the internal {@link JacksonTransformers} class which uses jackson for encoding and decoding
 * of JSON.</p>
 */
public class JsonSerializer implements Serializer {

  public static JsonSerializer create() {
    return new JsonSerializer();
  }

  private JsonSerializer() {}

  @Override
  public byte[] serialize(final Object input) {
    try {
      return JacksonTransformers.MAPPER.writeValueAsBytes(input);
    } catch (Throwable t) {
      throw new EncodingFailedException("Serializing of content + " + input + " to JSON failed.", t);
    }
  }

  @Override
  public <T> T deserialize(final Class<T> target, final byte[] input) {
    try {
      return JacksonTransformers.MAPPER.readValue(input, target);
    } catch (Throwable e) {
      if (e instanceof DecodingFailedException) {
        e = e.getCause();
      }
      throw new DecodingFailedException("Deserialization of content into target " + target
        + " failed; encoded = " + redactUser(new String(input, CharsetUtil.UTF_8)), e);
    }
  }

}
