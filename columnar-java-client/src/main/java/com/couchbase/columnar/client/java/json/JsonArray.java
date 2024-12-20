/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.columnar.client.java.json;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.columnar.client.java.internal.JacksonTransformers;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a JSON array that can be stored and loaded from Couchbase Server.
 * <p>
 * If boxed return values are unboxed, the calling code needs to make sure to handle potential
 * {@link NullPointerException}s.
 * <p>
 * The {@link JsonArray} is backed by a {@link List} and is intended to work similar to it API wise, but to only
 * allow to store such objects which can be represented by JSON.
 */
@SuppressWarnings("unused")
public final class JsonArray extends JsonValue implements Iterable<Object> {

  /**
   * The backing list of the array.
   */
  private final List<@Nullable Object> content;

  /**
   * Creates a new {@link JsonArray} with the default capacity.
   */
  private JsonArray() {
    content = new ArrayList<>();
  }

  /**
   * Creates a new {@link JsonArray} with a custom capacity.
   */
  private JsonArray(int initialCapacity) {
    content = new ArrayList<>(initialCapacity);
  }

  /**
   * Creates an empty {@link JsonArray}.
   *
   * @return an empty {@link JsonArray}.
   */
  public static JsonArray create() {
    return new JsonArray();
  }

  /**
   * Creates an empty {@link JsonArray}.
   *
   * @param initialCapacity the initial capacity for this json array.
   * @return an empty {@link JsonArray}.
   */
  public static JsonArray create(int initialCapacity) {
    return new JsonArray(initialCapacity);
  }

  /**
   * Creates a new {@link JsonArray} and populates it with the values supplied.
   *
   * @param items the items to be stored in the {@link JsonArray}.
   * @return a populated {@link JsonArray}.
   * @throws IllegalArgumentException if an element type is not supported.
   */
  public static JsonArray from(Object... items) {
    JsonArray array = new JsonArray(items.length);
    for (Object item : items) {
      array.add(JsonValue.coerce(item));
    }
    return array;
  }

  /**
   * Returns a new {@link JsonArray} containing items from the given {@link List},
   * in the same order they were returned by the list's iterator.
   * <p>
   * <b>Sub Maps and Iterables:</b>
   * If possible, contained Maps and Iterables are converted to JsonObject and
   * JsonArray respectively. However, some restrictions apply. Any non-convertible item
   * will raise a {@link ClassCastException}. If the sub-conversion raises an exception (like an
   * InvalidArgumentException) then it is put as cause for the ClassCastException.
   *
   * @param items the list of items to be stored in the {@link JsonArray}.
   * @return a new {@link JsonArray} containing the elements from the given list,
   * in the order they were returned by the list's iterator.
   * @throws IllegalArgumentException if one or more list elements is of unsupported type.
   * @throws NullPointerException if the given list is null.
   */
  public static JsonArray from(List<?> items) {
    JsonArray array = new JsonArray(items.size());
    for (Object item : items) {
      array.add(JsonValue.coerce(item));
    }
    return array;
  }

  /**
   * Returns a new {@link JsonArray} containing items from the given {@link Iterable},
   * in the same order they were returned by the iterable's iterator.
   * <p>
   * <b>Sub Maps and Iterables:</b>
   * If possible, contained Maps and Iterables are converted to JsonObject and
   * JsonArray respectively. However, some restrictions apply. Any non-convertible item
   * will raise a {@link ClassCastException}. If the sub-conversion raises an exception (like an
   * InvalidArgumentException) then it is put as cause for the ClassCastException.
   *
   * @param items the iterable to convert to a JsonArray.
   * @return a new {@link JsonArray} containing the elements from the given iterable,
   * in the order they were returned by the iterable's iterator.
   * @throws IllegalArgumentException if one or more elements is of unsupported type.
   * @throws NullPointerException if the given iterable is null.
   */
  @Stability.Internal
  static JsonArray fromIterable(Iterable<?> items) {
    JsonArray array = new JsonArray();
    for (Object it : items) {
      array.add(JsonValue.coerce(it));
    }
    return array;
  }

  /**
   * Static method to create a {@link JsonArray} from a JSON {@link String}.
   * <p>
   * Not to be confused with {@link #from(Object...)} from(aString) which will populate a new array with the string.
   * <p>
   * The string is expected to be a valid JSON array representation (eg. starting with a '[').
   *
   * @param jsonArray the JSON String to convert to a {@link JsonArray}.
   * @return the corresponding {@link JsonArray}.
   * @throws IllegalArgumentException if the given String is not valid JSON Array.
   */
  public static JsonArray fromJson(String jsonArray) {
    try {
      return JacksonTransformers.MAPPER.readValue(jsonArray, JsonArray.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot convert string to JsonArray", e);
    }
  }

  public static JsonArray fromJson(byte[] jsonArray) {
    try {
      return JacksonTransformers.MAPPER.readValue(jsonArray, JsonArray.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot convert byte array to JsonArray", e);
    }
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and does not cast it.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable Object get(int index) {
    return content.get(index);
  }

  /**
   * Append an element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   * @throws IllegalArgumentException if the value type is not supported.
   */
  public JsonArray add(@Nullable Object value) {
    if (value == this) {
      throw new IllegalArgumentException("Cannot add self");
    }
    content.add(coerce(value));
    return this;
  }

  /**
   * Append a null element to the {@link JsonArray}.
   * <p>
   * This is equivalent to calling {@link JsonArray#add(Object)} with null.
   *
   * @return the {@link JsonArray}.
   */
  public JsonArray addNull() {
    content.add(null);
    return this;
  }

  /**
   * Append an {@link String} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(@Nullable String value) {
    content.add(value);
    return this;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link String}.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable String getString(int index) {
    return (String) content.get(index);
  }

  /**
   * Append an {@link Long} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(long value) {
    content.add(value);
    return this;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link Long}.
   * <p>
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable Long getLong(int index) {
    Number n = (Number) content.get(index);
    if (n == null) {
      return null;
    } else if (n instanceof Long) {
      return (Long) n;
    } else {
      return n.longValue(); //autoboxing to Long
    }
  }

  /**
   * Append an {@link Integer} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(int value) {
    content.add(value);
    return this;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link Integer}.
   * <p>
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable Integer getInt(int index) {
    Number n = (Number) content.get(index);
    if (n == null) {
      return null;
    } else if (n instanceof Integer) {
      return (Integer) n;
    } else {
      return n.intValue(); //autoboxing to Integer
    }
  }

  /**
   * Append an {@link Double} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(double value) {
    content.add(value);
    return this;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link Double}.
   * <p>
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable Double getDouble(int index) {
    Number n = (Number) content.get(index);
    if (n == null) {
      return null;
    } else if (n instanceof Double) {
      return (Double) n;
    } else {
      return n.doubleValue(); //autoboxing to Double
    }
  }

  /**
   * Append an {@link Boolean} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(boolean value) {
    content.add(value);
    return this;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link Boolean}.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public boolean getBoolean(int index) {
    return (Boolean) content.get(index);
  }

  /**
   * Append an {@link JsonObject} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(@Nullable JsonObject value) {
    content.add(value);
    return this;
  }

  /**
   * Append an {@link JsonObject} element, converted from a {@link List}, to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   * @see JsonObject#from(Map)
   */
  public JsonArray add(Map<String, ?> value) {
    return add(JsonObject.from(value));
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link JsonObject}.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable JsonObject getObject(int index) {
    return (JsonObject) content.get(index);
  }

  /**
   * Append an {@link JsonArray} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(@Nullable JsonArray value) {
    if (value == this) {
      throw new IllegalArgumentException("Cannot add self");
    }
    content.add(value);
    return this;
  }

  /**
   * Append an {@link JsonArray} element, converted from a {@link List}, to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   * @see #from(List)
   */
  public JsonArray add(@Nullable List<?> value) {
    return value == null
      ? addNull()
      : add(JsonArray.from(value));
  }

  /**
   * Append a {@link Number} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(@Nullable Number value) {
    content.add(value);
    return this;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link JsonArray}.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable JsonArray getArray(int index) {
    return (JsonArray) content.get(index);
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link BigInteger}.
   *
   * @param index the index of the value.
   * @return the value at the given index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable BigInteger getBigInteger(int index) {
    return (BigInteger) content.get(index);
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link BigDecimal}.
   *
   * @param index the index of the value.
   * @return the value at the given index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable BigDecimal getBigDecimal(int index) {
    Object found = content.get(index);
    if (found == null) {
      return null;
    } else if (found instanceof Double) {
      return BigDecimal.valueOf((Double) found);
    }
    return (BigDecimal) found;
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link Number}.
   *
   * @param index the index of the value.
   * @return the value at the given index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public @Nullable Number getNumber(int index) {
    return (Number) content.get(index);
  }

  /**
   * Copies the content of the {@link JsonArray} into a new {@link List} and returns it.
   * Note that if the array contains sub-{@link JsonObject} or {@link JsonArray}, they
   * will recursively be converted to {@link Map} and {@link List}, respectively.
   *
   * @return the content of the {@link JsonArray} in a new {@link List}.
   */
  public List<@Nullable Object> toList() {
    List<@Nullable Object> copy = new ArrayList<>(content.size());
    for (Object o : content) {
      if (o instanceof JsonObject) {
        copy.add(((JsonObject) o).toMap());
      } else if (o instanceof JsonArray) {
        copy.add(((JsonArray) o).toList());
      } else {
        copy.add(o);
      }
    }
    return copy;
  }

  /**
   * Checks if the {@link JsonArray} is empty or not.
   *
   * @return true if it is, false otherwise.
   */
  public boolean isEmpty() {
    return content.isEmpty();
  }

  /**
   * Returns the size of the {@link JsonArray}.
   *
   * @return the size.
   */
  public int size() {
    return content.size();
  }

  /**
   * Returns true if the object is part of the array.
   *
   * @param value the value to check.
   * @return true if it is part of the array, false otherwise.
   */
  public boolean contains(@Nullable Object value) {
    return content.contains(value);
  }

  @Override
  public Iterator<Object> iterator() {
    return content.iterator();
  }

  /**
   * Converts the {@link JsonArray} into its JSON string representation.
   *
   * @return the JSON string representing this {@link JsonArray}.
   */
  @Override
  public String toString() {
    try {
      return JacksonTransformers.MAPPER.writeValueAsString(this);
    } catch (Exception e) {
      throw new RuntimeException("Cannot convert JsonArray to Json String", e);
    }
  }

  /**
   * Similar to {@link  #toString()} but turns this array directly into an encoded byte array.
   *
   * @return the byte array representing this {@link JsonArray}.
   */
  public byte[] toBytes() {
    try {
      return JacksonTransformers.MAPPER.writeValueAsBytes(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot convert JsonArray to Json byte array", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JsonArray objects = (JsonArray) o;
    return Objects.equals(content, objects.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(content);
  }

}
