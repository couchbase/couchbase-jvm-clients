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

package com.couchbase.client.java.json;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a JSON array that can be stored and loaded from Couchbase Server.
 *
 * If boxed return values are unboxed, the calling code needs to make sure to handle potential
 * {@link NullPointerException}s.
 *
 * The {@link JsonArray} is backed by a {@link List} and is intended to work similar to it API wise, but to only
 * allow to store such objects which can be represented by JSON.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @since 2.0
 */
public class JsonArray extends JsonValue implements Iterable<Object>, Serializable {

  private static final long serialVersionUID = 456072884048969058L;

  /**
   * The backing list of the array.
   */
  private final List<Object> content;

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
   * Creates a empty {@link JsonArray}.
   *
   * @return a empty {@link JsonArray}.
   */
  public static JsonArray create() {
    return new JsonArray();
  }

  /**
   * Creates a empty {@link JsonArray}.
   *
   * @param initialCapacity the initial capacity for this json array.
   * @return a empty {@link JsonArray}.
   */
  public static JsonArray create(int initialCapacity) {
    return new JsonArray(initialCapacity);
  }

  /**
   * Creates a new {@link JsonArray} and populates it with the values supplied.
   *
   * If the type is not supported, a {@link InvalidArgumentException} exception is thrown.
   * @param items the items to be stored in the {@link JsonArray}.
   * @return a populated {@link JsonArray}.
   */
  public static JsonArray from(Object... items) {
    JsonArray array = new JsonArray(items.length);
    for (Object item : items) {
      if (checkType(item)) {
        array.add(item);
      } else {
        throw InvalidArgumentException.fromMessage("Unsupported type for JsonArray: " + item.getClass());
      }
    }
    return array;
  }

  /**
   * Creates a new {@link JsonArray} and populates it with the values in the supplied {@link List}.
   *
   * If the type of an item is not supported, an {@link InvalidArgumentException} is thrown.
   * If the list is null, a {@link NullPointerException} is thrown, but null items are supported.
   *
   * *Sub Maps and Lists*
   * If possible, Maps and Lists contained in items will be converted to JsonObject and
   * JsonArray respectively. However, same restrictions apply. Any non-convertible collection
   * will raise a {@link ClassCastException}. If the sub-conversion raises an exception (like an
   * InvalidArgumentException) then it is put as cause for the ClassCastException.
   *
   * @param items the list of items to be stored in the {@link JsonArray}.
   * @return a populated {@link JsonArray}.
   * @throws InvalidArgumentException if at least one item is of unsupported type.
   */
  @SuppressWarnings("unchecked")
  public static JsonArray from(List<?> items) {
    if (items == null) {
      throw InvalidArgumentException.fromMessage("Null list unsupported");
    } else if (items.isEmpty()) {
      return JsonArray.create();
    }

    JsonArray array = new JsonArray(items.size());
    ListIterator<?> iter = items.listIterator();
    while (iter.hasNext()) {
      int i = iter.nextIndex();
      Object item = iter.next();

      if (item == JsonValue.NULL) {
        item = null;
      }

      if (item instanceof Map) {
        try {
          JsonObject sub = JsonObject.from((Map<String, ?>) item);
          array.add(sub);
        } catch (ClassCastException e) {
          throw e;
        } catch (Exception e) {
          ClassCastException c = new ClassCastException("Couldn't convert sub-Map at " + i + " to JsonObject");
          c.initCause(e);
          throw c;
        }
      } else if (item instanceof List) {
        try {
          JsonArray sub = JsonArray.from((List<?>) item);
          array.add(sub);
        } catch (Exception e) {
          //no risk of a direct ClassCastException here
          ClassCastException c = new ClassCastException(
            "Couldn't convert sub-List at " + i + " to JsonArray");
          c.initCause(e);
          throw c;
        }
      } else if (checkType(item)) {
        array.add(item);
      } else {
        throw InvalidArgumentException.fromMessage("Unsupported type for JsonArray: " + item.getClass());
      }
    }
    return array;
  }

  /**
   * Static method to create a {@link JsonArray} from a JSON {@link String}.
   *
   * Not to be confused with {@link #from(Object...)} from(aString)} which will populate a new array with the string.
   *
   * The string is expected to be a valid JSON array representation (eg. starting with a '[').
   *
   * @param s the JSON String to convert to a {@link JsonArray}.
   * @return the corresponding {@link JsonArray}.
   * @throws InvalidArgumentException if the conversion cannot be done.
   */
  public static JsonArray fromJson(String s) {
    try {
      return JacksonTransformers.stringToJsonArray(s);
    } catch (Exception e) {
      throw InvalidArgumentException.fromMessage("Cannot convert string to JsonArray", e);
    }
  }

  public static JsonArray fromJson(byte[] s) {
    try {
      return JacksonTransformers.bytesToJsonArray(s);
    } catch (Exception e) {
      throw InvalidArgumentException.fromMessage("Cannot convert byte array to JsonArray", e);
    }
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and does not cast it.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public Object get(int index) {
    return content.get(index);
  }

  /**
   * Append an element to the {@link JsonArray}.
   *
   * Note that the type is checked and a {@link InvalidArgumentException} is thrown if not supported.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(Object value) {
    if (value == this) {
      throw InvalidArgumentException.fromMessage("Cannot add self");
    } else if (value == JsonValue.NULL) {
      addNull();
    } else if (checkType(value)) {
      content.add(value);
    } else {
      throw InvalidArgumentException.fromMessage("Unsupported type for JsonArray: " + value.getClass());
    }
    return this;
  }

  /**
   * Append a null element to the {@link JsonArray}.
   *
   * This is equivalent to calling {@link JsonArray#add(Object)} with null or {@link JsonValue#NULL}.
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
  public JsonArray add(String value) {
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
  public String getString(int index) {
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
   *
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public Long getLong(int index) {
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
   *
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public Integer getInt(int index) {
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
   *
   * Note that if value was stored as another numerical type, some truncation or rounding may occur.
   *
   * @param index the index of the value.
   * @return the value at index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public Double getDouble(int index) {
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
  public JsonArray add(JsonObject value) {
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
  public JsonObject getObject(int index) {
    return (JsonObject) content.get(index);
  }

  /**
   * Append an {@link JsonArray} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(JsonArray value) {
    if (value == this) {
      throw InvalidArgumentException.fromMessage("Cannot add self");
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
  public JsonArray add(List<?> value) {
    return add(JsonArray.from(value));
  }

  /**
   * Append a {@link Number} element to the {@link JsonArray}.
   *
   * @param value the value to append.
   * @return the {@link JsonArray}.
   */
  public JsonArray add(Number value) {
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
  public JsonArray getArray(int index) {
    return (JsonArray) content.get(index);
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link BigInteger}.
   *
   * @param index the index of the value.
   * @return the value at the given index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public BigInteger getBigInteger(int index) {
    return (BigInteger) content.get(index);
  }

  /**
   * Retrieves the value by the position in the {@link JsonArray} and casts it to {@link BigDecimal}.
   *
   * @param index the index of the value.
   * @return the value at the given index.
   * @throws IndexOutOfBoundsException if the index is negative or too large.
   */
  public BigDecimal getBigDecimal(int index) {
    Object found = content.get(index);
    if (found == null) {
      return null;
    } else if (found instanceof Double) {
      return new BigDecimal((Double) found);
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
  public Number getNumber(int index) {
    return (Number) content.get(index);
  }

  /**
   * Copies the content of the {@link JsonArray} into a new {@link List} and returns it.
   * Note that if the array contains sub-{@link JsonObject} or {@link JsonArray}, they
   * will recursively be converted to {@link Map} and {@link List}, respectively.
   *
   * @return the content of the {@link JsonArray} in a new {@link List}.
   */
  public List<Object> toList() {
    List<Object> copy = new ArrayList<>(content.size());
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
  public boolean contains(Object value) {
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
      throw new IllegalStateException("Cannot convert JsonArray to Json String", e);
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
      throw new IllegalStateException("Cannot convert JsonArray to Json byte array", e);
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