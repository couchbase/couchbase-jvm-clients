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

/**
 * The {@link JsonSerializer} handles the serialization and deserialization of raw json data into java objects.
 */
public interface JsonSerializer {

  /**
   * Serializes the given input into its encoded byte array form.
   *
   * @param input the object as input.
   * @return the serialized output.
   */
  byte[] serialize(Object input);

  /**
   * Deserializes raw input into the target class.
   *
   * @param target the target class.
   * @param input the raw input.
   * @param <T> the generic type to deserialize into.
   * @return the deserialized output.
   */
  <T> T deserialize(Class<T> target, byte[] input);

  /**
   * Deserializes raw input into the target type.
   *
   * @param target the target type.
   * @param input the raw input.
   * @param <T> the type to deserialize into.
   * @return the deserialized output.
   */
  default <T> T deserialize(TypeRef<T> target, byte[] input) {
    throw new DecodingFailureException(getClass().getSimpleName() + " does not support decoding via TypeRef.");
  }
}
