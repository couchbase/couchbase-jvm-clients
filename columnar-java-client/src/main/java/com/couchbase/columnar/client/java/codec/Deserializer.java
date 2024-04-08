/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.client.java.codec;

import reactor.util.annotation.Nullable;

import java.io.IOException;

/**
 * Converts query result rows into Java objects.
 */
public interface Deserializer {
  /**
   * Deserializes raw input into the target class.
   *
   * @param target the target class.
   * @param input the raw input.
   * @param <T> the generic type to deserialize into.
   * @return the deserialized output.
   */
  @Nullable
  <T> T deserialize(Class<T> target, byte[] input) throws IOException;

  /**
   * Deserializes raw input into the target type.
   *
   * @param target the target type.
   * @param input the raw input.
   * @param <T> the type to deserialize into.
   * @return the deserialized output.
   */
  @Nullable
  <T> T deserialize(TypeRef<T> target, byte[] input) throws IOException;
}
