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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.util.collection.IntObjectHashMap;
import com.couchbase.client.core.deps.io.netty.util.collection.IntObjectMap;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static com.couchbase.client.core.util.CompactIntMap.MAX_WASTED_ARRAY_ELEMENTS;
import static java.util.Objects.requireNonNull;

/**
 * A read-only map where the keys are non-negative integers.
 */
@Stability.Internal
public interface IntMap<E> {
  /**
   * Returns the value associated with the given integer, or null if not found.
   *
   * @param key the non-negative integer associated with the value to return
   * @throws IllegalArgumentException if the given key is negative.
   */
  @Nullable
  E get(int key);

  /**
   * Reinterprets the short as a positive integer, then returns the value
   * associated with the given integer, or null if not found.
   *
   * @param key the short associated with the value to return
   */
  @Nullable
  default E get(short key) {
    // reinterpret as positive integer
    return get(key & 0xffff);
  }

  /**
   * Reinterprets the byte as a positive integer, then returns the value
   * associated with the given integer, or null if not found.
   *
   * @param key the byte associated with the value to return
   */
  @Nullable
  default E get(byte key) {
    // reinterpret as positive integer
    return get(key & 0xff);
  }

  /**
   * Returns a new instance where the values are the values of the given enum class,
   * and the associated key is derived by applying the given function to the value.
   *
   * @throws IllegalArgumentException if a derived key is negative, null, or not unique
   */
  static <E extends Enum<?>> IntMap<E> from(
    Class<E> enumClass,
    Function<E, Integer> keyExtractor
  ) {
    return from(Arrays.asList(enumClass.getEnumConstants()), keyExtractor);
  }

  /**
   * Returns a new instance where the values are the values of the given iterable,
   * and the associated key is derived by applying the given function to the value.
   *
   * @throws IllegalArgumentException if a derived key is negative, null, or not unique
   */
  static <E> IntMap<E> from(
    Iterable<E> items,
    Function<E, Integer> keyExtractor
  ) {
    requireNonNull(keyExtractor);

    Map<Integer, E> map = new HashMap<>();
    for (E value : items) {
      Integer key = keyExtractor.apply(value);
      E existing = map.put(key, value);
      if (existing != null) {
        throw new IllegalArgumentException("Generated keys for IntMap must be unique, but " + key + " was mapped to both '" + existing + "' and '" + value + "'.");
      }
    }
    return from(map);
  }

  /**
   * Returns a new instance from the entries of the given map.
   *
   * @throws IllegalArgumentException if the map contains a key that is null or negative.
   */
  static <E> IntMap<E> from(Map<Integer, E> map) {
    if (map.isEmpty()) {
      return CompactIntMap.empty();
    }

    for (Integer key : map.keySet()) {
      if (key == null || key < 0) {
        throw new IllegalArgumentException("IntMap key must be non-null and non-negative, but got: " + key);
      }
    }

    int maxIndex = map.keySet().stream().max(Integer::compareTo).get();
    int wastedArrayElements = (maxIndex + 1) - map.size();

    if (wastedArrayElements > MAX_WASTED_ARRAY_ELEMENTS) {
      return new SparseIntMap<>(map);
    }

    Object[] indexToValue = new Object[maxIndex + 1];
    map.forEach((key,value) -> indexToValue[key] = value);

    return new CompactIntMap<>(indexToValue);
  }
}

class CompactIntMap<E> implements IntMap<E> {
  static final int MAX_WASTED_ARRAY_ELEMENTS = 1024;

  private static final IntMap<?> EMPTY = new CompactIntMap<>(new Object[0]);

  @SuppressWarnings("unchecked")
  static <E> IntMap<E> empty() {
    return (IntMap<E>) EMPTY;
  }

  private final E[] indexToValue;

  @SuppressWarnings("unchecked")
  CompactIntMap(Object[] indexToValue) {
    this.indexToValue = (E[]) indexToValue;
  }

  @Override
  public E get(int key) {
    try {
      if (key >= indexToValue.length) return null;
      return indexToValue[key];
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("key must be non-negative, but got " + key);
    }
  }

  private Map<Integer, E> toMap() {
    Map<Integer, E> result = new LinkedHashMap<>();
    for (int i = 0; i < indexToValue.length; i++) {
      if (get(i) != null) {
        result.put(i, get(i));
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "CompactIntMap" + toMap();
  }
}

class SparseIntMap<E> implements IntMap<E> {
  private final IntObjectMap<E> indexToValue;

  public SparseIntMap(Map<Integer, E> indexToValue) {
    this.indexToValue = new IntObjectHashMap<>(indexToValue.size());
    this.indexToValue.putAll(indexToValue);
  }

  @Override
  public E get(int key) {
    E result = indexToValue.get(key);
    if (result == null && key < 0) {
      throw new IllegalArgumentException("key must be non-negative, but got " + key);
    }
    return result;
  }

  @Override
  public String toString() {
    return "SparseIntMap" + new TreeMap<>(indexToValue);
  }
}
