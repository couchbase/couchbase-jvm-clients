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

package com.couchbase.columnar.client.java;

import com.couchbase.columnar.client.java.codec.Deserializer;
import com.couchbase.columnar.client.java.codec.TypeRef;
import com.couchbase.columnar.client.java.internal.InternalJacksonSerDes;
import com.couchbase.columnar.client.java.internal.ThreadSafe;
import com.couchbase.columnar.client.java.json.JsonArray;
import com.couchbase.columnar.client.java.json.JsonObject;
import com.couchbase.columnar.client.java.json.JsonValue;
import reactor.util.annotation.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * One row of a query result.
 * <p>
 * Convert the row's content to a Java object by calling {@link #as}
 * or {@link #asNullable}.
 * <p>
 * Conversion is done by the {@link Deserializer} specified in the query
 * options, or by the cluster's default deserializer if none was specified
 * for the query.
 * <p>
 * If you use {@code JacksonDeserializer} (the default) and enjoy
 * working with Jackson's tree model, call {@code row.as(JsonNode.class)}
 * to convert the row to a Jackson tree. Alternatively, pass a class whose
 * fields match the structure of the row, and let Jackson bind the row data
 * to a new instance of the class.
 * <p>
 * Convert to a parameterized type by calling the overload that
 * takes a {@link TypeRef} instead of a Class. For example:
 * <pre>
 * row.as(new TypeRef&lt;Map&lt;String, MyCustomClass>>() {});
 * </pre>
 * <p>
 * If you prefer to use the Couchbase Columnar SDK's simple {@link JsonValue}
 * tree model, convert the row to {@link JsonObject} or {@link JsonArray}.
 * These classes are not as full-featured as Jackson's tree model, but they
 * are always supported regardless of which deserializer you use.
 * <p>
 * Call {@link #bytes()} to get the raw bytes of the row content,
 * exactly as it was received from the server. This may be useful for
 * debugging, or if you want to handle deserialization yourself.
 *
 * @see ClusterOptions#deserializer(Deserializer)
 * @see QueryOptions#deserializer(Deserializer)
 */
@ThreadSafe(caveat = "Unless you modify the byte array returned by Row.bytes()")
public final class Row {
  private final byte[] content;
  private final Deserializer deserializer;

  Row(
    byte[] content,
    Deserializer deserializer
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
   * Returns the content of the row deserialized as an instance of the specified type.
   *
   * @throws NullPointerException if the row content is a literal null.
   * @throws DataConversionException if the row could not be deserialized
   * into an instance of the specified type.
   */
  public <T> T as(Class<T> type) {
    return requireNonNull(
      asNullable(type),
      "Row content was a literal null. If this is expected, call `row.asNullable(type)` instead of `row.as(type)`."
    );
  }

  /**
   * Returns the content of the row deserialized as an instance of the specified type,
   * or null if the row's content is a literal null.
   *
   * @throws DataConversionException if the row could not be deserialized
   * into an instance of the specified type.
   */
  @Nullable
  public <T> T asNullable(Class<T> type) {
    requireNonNull(type, "`type` argument must be non-null");
    try {
      // Fail fast with a helpful message if user tries to
      // use ObjectNode from shadowed Jackson, for example.
      String corePackage = "com.couchbase.client.core";
      if (type.getName().startsWith(corePackage)) {
        throw new IllegalArgumentException("Classes in internal API package '" + corePackage + "' are not valid deserialization targets. Did you mean to import this class from a different package? " + type);
      }

      // If type is Couchbase JsonObject or JsonArray,
      // always use a serializer that knows how to handle them.
      return JsonValue.class.isAssignableFrom(type)
        ? InternalJacksonSerDes.INSTANCE.deserialize(type, content)
        : deserializer.deserialize(type, content);

    } catch (Exception e) {
      throw new DataConversionException(e);
    }
  }

  /**
   * Returns the content of the row deserialized as an instance of the specified type.
   *
   * @throws NullPointerException if the row content is a literal null.
   * @throws DataConversionException if the row could not be deserialized
   * into an instance of the specified type.
   */
  public <T> T as(TypeRef<T> type) {
    return requireNonNull(
      asNullable(type),
      "Row content was a literal null. If this is expected, call `row.asNullable(type)` instead of `row.as(type)`."
    );
  }

  /**
   * Returns the content of the row deserialized as an instance of the specified type,
   * or null if the row content is a literal null.
   *
   * @throws DataConversionException if the row could not be deserialized
   * into an instance of the specified type.
   */
  @Nullable
  public <T> T asNullable(TypeRef<T> type) {
    requireNonNull(type, "`type` argument must be non-null");
    try {
      return deserializer.deserialize(type, content);
    } catch (Exception e) {
      throw new DataConversionException(e);
    }
  }

  @Override
  public String toString() {
    return new String(content, UTF_8);
  }
}
