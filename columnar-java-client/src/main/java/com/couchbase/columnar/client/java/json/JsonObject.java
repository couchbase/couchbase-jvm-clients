/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.columnar.client.java.internal.JacksonTransformers;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Represents a JSON object that can be stored and loaded from Couchbase Server.
 * <p>
 * If boxed return values are unboxed, the calling code needs to make sure to handle potential
 * {@link NullPointerException}s.
 * <p>
 * The {@link JsonObject} is backed by a {@link Map} and is intended to work similar to it API wise, but to only
 * allow to store such objects which can be represented by JSON.
 */
@SuppressWarnings("unused")
public final class JsonObject extends JsonValue {

  /**
   * The backing {@link Map} for the object.
   */
  private final Map<String, @Nullable Object> content;

  /**
   * Private constructor to create the object.
   * <p>
   * The internal map is initialized with the default capacity.
   */
  private JsonObject() {
    content = new HashMap<>();
  }

  /**
   * Private constructor to create the object with a custom initial capacity.
   */
  private JsonObject(int initialCapacity) {
    content = new HashMap<>(initialCapacity);
  }

  /**
   * Creates an empty {@link JsonObject}.
   *
   * @return an empty {@link JsonObject}.
   */
  public static JsonObject create() {
    return new JsonObject();
  }

  /**
   * Creates an empty {@link JsonObject}.
   *
   * @param initialCapacity the initial capacity for the object.
   * @return an empty {@link JsonObject}.
   */
  public static JsonObject create(int initialCapacity) {
    return new JsonObject(initialCapacity);
  }

  /**
   * Constructs a {@link JsonObject} from a {@link Map Map&lt;String, ?&gt;}.
   * <p>
   * This is only possible if the given Map is well-formed, that is it contains non-null
   * keys, and all values are of a supported type.
   * <p>
   * A null input Map or null key will lead to a {@link NullPointerException} being thrown.
   * If any unsupported value is present in the Map, an {@link IllegalArgumentException}
   * will be thrown.
   * <p>
   * *Sub Maps and Lists*
   * If possible, Maps and Lists contained in mapData will be converted to JsonObject and
   * JsonArray respectively. However, same restrictions apply. Any non-convertible collection
   * will raise a {@link ClassCastException}. If the sub-conversion raises an exception (like an
   * InvalidArgumentException) then it is put as cause for the ClassCastException.
   *
   * @param mapData the Map to convert to a JsonObject
   * @return the resulting JsonObject
   * @throws IllegalArgumentException in case one or more unsupported values are present
   * @throws NullPointerException in case a null map is provided or if it contains a null key
   * @throws ClassCastException if map contains a sub-Map or sub-List not supported (see above)
   */
  public static JsonObject from(final Map<String, ?> mapData) {
    requireNonNull(mapData, "Null input Map unsupported");

    JsonObject result = new JsonObject(mapData.size());
    try {
      mapData.forEach((key, value) -> {
        requireNonNull(key, "The key is not allowed to be null");
        result.put(key, coerce(value));
      });
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Map key must be String", e);
    }

    return result;
  }

  /**
   * Static method to create a {@link JsonObject} from a JSON {@link String}.
   * <p>
   * The string is expected to be a valid JSON object representation (eg. starting with a '{').
   *
   * @param jsonObject the JSON String to convert to a {@link JsonObject}.
   * @return the corresponding {@link JsonObject}.
   * @throws IllegalArgumentException if the conversion cannot be done.
   */
  public static JsonObject fromJson(final String jsonObject) {
    try {
      return JacksonTransformers.MAPPER.readValue(jsonObject, JsonObject.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot convert string to JsonObject", e);
    }
  }

  public static JsonObject fromJson(final byte[] jsonObject) {
    try {
      return JacksonTransformers.MAPPER.readValue(jsonObject, JsonObject.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot convert byte array to JsonObject", e);
    }
  }

  /**
   * Stores a {@link Object} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   * @throws IllegalArgumentException if the value is not a supported JSON type
   */
  public JsonObject put(final String name, @Nullable final Object value) {
    if (this == value) {
      throw new IllegalArgumentException("Cannot put self");
    }
    content.put(name, coerce(value));
    return this;
  }

  /**
   * Retrieves the (potential null) content and not casting its type.
   *
   * @param name the key of the field.
   * @return the value of the field, or null if it does not exist.
   */
  public @Nullable Object get(final String name) {
    return content.get(name);
  }

  /**
   * Stores a {@link String} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(final String name, @Nullable final String value) {
    content.put(name, value);
    return this;
  }

  /**
   * Retrieves the value from the field name and casts it to {@link String}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable String getString(String name) {
    return (String) content.get(name);
  }

  /**
   * Stores a {@link Integer} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, int value) {
    content.put(name, value);
    return this;
  }

  /**
   * Retrieves the value from the field name and casts it to {@link Integer}.
   * <p>
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable Integer getInt(String name) {
    //let it fail in the more general case where it isn't actually a number
    Number number = (Number) content.get(name);
    if (number == null) {
      return null;
    } else if (number instanceof Integer) {
      return (Integer) number;
    } else {
      return number.intValue(); //autoboxing to Integer
    }
  }

  /**
   * Stores a {@link Long} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, long value) {
    content.put(name, value);
    return this;
  }

  /**
   * Retrieves the value from the field name and casts it to {@link Long}.
   * <p>
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable Long getLong(String name) {
    //let it fail in the more general case where it isn't actually a number
    Number number = (Number) content.get(name);
    if (number == null) {
      return null;
    } else if (number instanceof Long) {
      return (Long) number;
    } else {
      return number.longValue(); //autoboxing to Long
    }
  }

  /**
   * Stores a {@link Double} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, double value) {
    content.put(name, value);
    return this;
  }

  /**
   * Retrieves the value from the field name and casts it to {@link Double}.
   * <p>
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable Double getDouble(String name) {
    //let it fail in the more general case where it isn't actually a number
    Number number = (Number) content.get(name);
    if (number == null) {
      return null;
    } else if (number instanceof Double) {
      return (Double) number;
    } else {
      return number.doubleValue(); //autoboxing to Double
    }
  }

  /**
   * Stores a {@link Boolean} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, boolean value) {
    content.put(name, value);
    return this;
  }

  /**
   * Retrieves the value from the field name and casts it to {@link Boolean}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable Boolean getBoolean(String name) {
    return (Boolean) content.get(name);
  }

  /**
   * Stores a {@link JsonObject} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, @Nullable JsonObject value) {
    if (this == value) {
      throw new IllegalArgumentException("Cannot put self");
    }
    content.put(name, value);
    return this;
  }

  /**
   * Attempt to convert a {@link Map} to a {@link JsonObject} value and store it, identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   * @see #from(Map)
   */
  public JsonObject put(String name, @Nullable Map<String, ?> value) {
    return put(name, value == null ? null : JsonObject.from(value));
  }

  /**
   * Retrieves the value from the field name and casts it to {@link JsonObject}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable JsonObject getObject(String name) {
    return (JsonObject) content.get(name);
  }

  /**
   * Stores a {@link JsonArray} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, @Nullable JsonArray value) {
    content.put(name, value);
    return this;
  }

  /**
   * Stores a {@link Number} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, @Nullable Number value) {
    content.put(name, value);
    return this;
  }

  /**
   * Stores a {@link JsonArray} value identified by the field name.
   *
   * @param name the name of the JSON field.
   * @param value the value of the JSON field.
   * @return the {@link JsonObject}.
   */
  public JsonObject put(String name, @Nullable List<?> value) {
    return put(name, value == null ? null : JsonArray.from(value));
  }

  /**
   * Retrieves the value from the field name and casts it to {@link JsonArray}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable JsonArray getArray(String name) {
    return (JsonArray) content.get(name);
  }

  /**
   * Retrieves the value from the field name and casts it to {@link BigInteger}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable BigInteger getBigInteger(String name) {
    return (BigInteger) content.get(name);
  }

  /**
   * Retrieves the value from the field name and casts it to {@link BigDecimal}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable BigDecimal getBigDecimal(String name) {
    Object found = content.get(name);
    if (found == null) {
      return null;
    } else if (found instanceof Double) {
      return BigDecimal.valueOf((Double) found);
    }
    return (BigDecimal) found;
  }

  /**
   * Retrieves the value from the field name and casts it to {@link Number}.
   *
   * @param name the name of the field.
   * @return the result or null if it does not exist.
   */
  public @Nullable Number getNumber(String name) {
    return (Number) content.get(name);
  }

  /**
   * Store a null value identified by the field's name.
   * <p>
   * This method is equivalent to calling {@link #put(String, Object)} with
   * a null value explicitly cast to Object.
   *
   * @param name The null field's name.
   * @return the {@link JsonObject}
   */
  public JsonObject putNull(String name) {
    content.put(name, null);
    return this;
  }

  /**
   * Removes an entry from the {@link JsonObject}.
   *
   * @param name the name of the field to remove
   * @return the {@link JsonObject}
   */
  public JsonObject removeKey(String name) {
    content.remove(name);
    return this;
  }

  /**
   * Returns a set of field names on the {@link JsonObject}.
   *
   * @return the set of names on the object.
   */
  public Set<String> getNames() {
    return content.keySet();
  }

  /**
   * Returns true if the {@link JsonObject} is empty, false otherwise.
   *
   * @return true if empty, false otherwise.
   */
  public boolean isEmpty() {
    return content.isEmpty();
  }

  /**
   * Transforms the {@link JsonObject} into a {@link Map}. The resulting
   * map is not backed by this {@link JsonObject}, and all sub-objects or
   * sub-arrays ({@link JsonArray}) are also recursively converted to
   * maps and lists, respectively.
   *
   * @return the content copied as a {@link Map}.
   */
  public Map<String, @Nullable Object> toMap() {
    Map<String, @Nullable Object> copy = new HashMap<>(content.size());
    for (Map.Entry<String, Object> entry : content.entrySet()) {
      Object content = entry.getValue();
      if (content instanceof JsonObject) {
        copy.put(entry.getKey(), ((JsonObject) content).toMap());
      } else if (content instanceof JsonArray) {
        copy.put(entry.getKey(), ((JsonArray) content).toList());
      } else {
        copy.put(entry.getKey(), content);
      }
    }
    return copy;
  }

  /**
   * Checks if the {@link JsonObject} contains the field name.
   *
   * @param name the name of the field.
   * @return true if its contained, false otherwise.
   */
  public boolean containsKey(String name) {
    return content.containsKey(name);
  }

  /**
   * Checks if the {@link JsonObject} contains the value.
   *
   * @param value the actual value.
   * @return true if its contained, false otherwise.
   */
  public boolean containsValue(Object value) {
    return content.containsValue(value);
  }

  /**
   * The size of the {@link JsonObject}.
   *
   * @return the size.
   */
  public int size() {
    return content.size();
  }

  /**
   * Converts the {@link JsonObject} into its JSON string representation.
   *
   * @return the JSON string representing this {@link JsonObject}.
   */
  @Override
  public String toString() {
    try {
      return JacksonTransformers.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot convert JsonObject to Json String", e);
    }
  }

  /**
   * Similar to {@link  #toString()} but turns this object directly into an encoded byte array.
   *
   * @return the byte array representing this {@link JsonObject}.
   */
  public byte[] toBytes() {
    try {
      return JacksonTransformers.MAPPER.writeValueAsBytes(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot convert JsonObject to Json byte array", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JsonObject that = (JsonObject) o;
    return Objects.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(content);
  }
}
