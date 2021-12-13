/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.codec;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonValue;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Wraps another serializer, intercepting and handling requests to
 * [de]serialize JsonObject and JsonArray.
 */
public class JsonValueSerializerWrapper implements JsonSerializer {
  private final JsonSerializer wrapped;

  public JsonValueSerializerWrapper(JsonSerializer wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  @Override
  public byte[] serialize(Object input) {
    if (input instanceof JsonValue) {
      try {
        return JacksonTransformers.MAPPER.writeValueAsBytes(input);
      } catch (Exception e) {
        throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", e);
      }
    }
    return wrapped.serialize(input);
  }

  @Override
  public <T> T deserialize(Class<T> target, byte[] input) {
    if (JsonValue.class.isAssignableFrom(target)) {
      try {
        return JacksonTransformers.MAPPER.readValue(input, target);
      } catch (Exception e) {
        throw new DecodingFailureException("Deserialization of content into target " + target
            + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
      }
    }
    return wrapped.deserialize(target, input);
  }

  @Override
  public <T> T deserialize(TypeRef<T> target, byte[] input) {
    return wrapped.deserialize(target, input);
  }
}
