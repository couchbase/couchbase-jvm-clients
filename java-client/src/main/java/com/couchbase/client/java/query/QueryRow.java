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

package com.couchbase.client.java.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * One row of a SQL++ query result.
 *
 * @see com.couchbase.client.java.Cluster#queryStreaming
 * @see com.couchbase.client.java.Scope#queryStreaming
 */
@Stability.Volatile
public final class QueryRow {
  private final byte[] content;
  private final JsonSerializer deserializer;

  @Stability.Internal
  public QueryRow(
    byte[] content,
    JsonSerializer deserializer
  ) {
    this.content = requireNonNull(content);
    this.deserializer = requireNonNull(deserializer);
  }

  /**
   * Returns the raw content of the row, exactly as it was received from
   * the server.
   * <p>
   * This method returns the same array each time it is called.
   */
  public byte[] bytes() {
    return content;
  }

  /**
   * Returns the content of the row deserialized as a {@link JsonObject},
   * or null if the row content is a literal null.
   *
   * @throws DecodingFailureException if the row could not be deserialized
   * into an instance of the specified type.
   */
  public JsonObject contentAsObject() {
    return contentAs(JsonObject.class);
  }

  /**
   * Returns the content of the row deserialized as an instance of the specified type,
   * or null if the row's content is a literal null.
   *
   * @throws DecodingFailureException if the row could not be deserialized
   * into an instance of the specified type.
   */
  public <T> T contentAs(Class<T> type) {
    requireNonNull(type, "`type` argument must be non-null");
    try {
      // Fail fast with a helpful message if user tries to
      // use ObjectNode from shadowed Jackson, for example.
      String corePackage = "com.couchbase.client.core";
      if (type.getName().startsWith(corePackage)) {
        throw new IllegalArgumentException("Classes in internal API package '" + corePackage + "' are not valid deserialization targets. Did you mean to import this class from a different package? " + type);
      }

      return deserializer.deserialize(type, content);

    } catch (Exception e) {
      throw new DecodingFailureException(e);
    }
  }

  /**
   * Returns the content of the row deserialized as an instance of the specified type,
   * or null if the row content is a literal null.
   *
   * @throws DecodingFailureException if the row could not be deserialized
   * into an instance of the specified type.
   */
  public <T> T contentAs(TypeRef<T> type) {
    requireNonNull(type, "`type` argument must be non-null");
    try {
      return deserializer.deserialize(type, content);
    } catch (Exception e) {
      throw new DecodingFailureException(e);
    }
  }

  @Override
  public String toString() {
    return new String(content, UTF_8);
  }
}
