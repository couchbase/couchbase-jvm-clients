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
import org.jspecify.annotations.Nullable;

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
  default @Nullable E get(short key) {
    // reinterpret as positive integer
    return get(key & 0xffff);
  }

  /**
   * Reinterprets the byte as a positive integer, then returns the value
   * associated with the given integer, or null if not found.
   *
   * @param key the byte associated with the value to return
   */
  default @Nullable E get(byte key) {
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
   * @throws IllegalArgumentException if the map contains a key that is null or negative,
   * or a value that is null.
   */
  static <E> IntMap<E> from(Map<Integer, E> map) {
    if (map.isEmpty()) {
      return CompactIntMap.empty();
    }

    // A null value would be indistinguishable from a missing key, since get() returns null for both.
    map.forEach((key, value) -> {
      //noinspection ConstantValue
      if (key == null || key < 0) {
        throw new IllegalArgumentException("IntMap key must be non-null and non-negative, but got: " + key);
      }
      if (value == null) {
        throw new IllegalArgumentException("IntMap values must be non-null, but key " + key + " has a null value.");
      }
    });

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
  public @Nullable E get(int key) {
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
      E value = get(i);
      if (value != null) {
        result.put(i, value);
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "CompactIntMap" + toMap();
  }
}

/**
 * For keys too far apart for {@link CompactIntMap}: a read-only hash table with open addressing and linear probing.
 * <p>
 * The table size is a power of two at least twice the number of entries, so the table is never more than half full,
 * and a lookup usually checks only one or two slots. A key's first slot comes from Fibonacci hashing
 * (multiply by a constant and keep the top bits), which spreads out patterned keys like multiples of a power of two.
 * <p>
 * An empty slot holds the key {@value #EMPTY}. That can't clash with a real key, because keys are non-negative.
 */
class SparseIntMap<E> implements IntMap<E> {
  private static final int EMPTY = -1;

  /**
   * 2^32 divided by the golden ratio, rounded to the nearest integer.
   * It's odd, so multiplying by it (mod 2^32) never maps two different keys to the same product.
   */
  private static final int FIBONACCI_MULTIPLIER = 0x9E3779B9;

  private final int[] keys;
  private final E[] values;

  /**
   * How far to shift a key's hash right to get a slot index in {@code [0, keys.length)}.
   */
  private final int shift;

  @SuppressWarnings("unchecked")
  SparseIntMap(Map<Integer, E> indexToValue) {
    int capacity = capacityFor(indexToValue.size());
    this.keys = new int[capacity];
    this.values = (E[]) new Object[capacity];
    this.shift = Integer.SIZE - Integer.numberOfTrailingZeros(capacity);
    Arrays.fill(keys, EMPTY);

    int mask = capacity - 1;
    indexToValue.forEach((key, value) -> {
      //noinspection ConstantValue
      if (key == null || key < 0) {
        // A key of -1 would be indistinguishable from an empty slot, and other negative keys could never be looked up.
        throw new IllegalArgumentException("SparseIntMap key must be non-null and non-negative, but got: " + key);
      }
      if (value == null) {
        throw new IllegalArgumentException("SparseIntMap values must be non-null, but key " + key + " has a null value.");
      }
      int i = slot(key);
      while (keys[i] != EMPTY) {
        i = (i + 1) & mask;
      }
      keys[i] = key;
      values[i] = value;
    });
  }

  /**
   * Returns the smallest power of two that is at least twice the given size (and at least 2),
   * so the table is never more than half full and always has an empty slot.
   * <p>
   * @implNote The minimum of 2 matters: a capacity of 1 would make {@link #shift} 32, and Java uses only
   * the low 5 bits of a shift count, so {@code >>> 32} wouldn't shift at all and {@link #slot} would
   * return an index far outside the table.
   */
  static int capacityFor(int size) {
    if (size < 0 || size > (1 << 29)) {
      throw new IllegalArgumentException("Unsupported size: " + size);
    }
    return Math.max(2, Integer.highestOneBit(size * 2 - 1) << 1);
  }

  /**
   * Returns the slot where a search for the given key starts.
   */
  int slot(int key) {
    return (key * FIBONACCI_MULTIPLIER) >>> shift;
  }

  /**
   * Returns the number of slots in the table.
   */
  int capacity() {
    return keys.length;
  }

  @Override
  public @Nullable E get(int key) {
    // Without this check, get(-1) would match an empty slot (EMPTY is -1) and return null instead of throwing.
    if (key < 0) {
      throw new IllegalArgumentException("key must be non-negative, but got " + key);
    }
    int mask = keys.length - 1;
    for (int i = slot(key); ; i = (i + 1) & mask) {
      int k = keys[i];
      if (k == key) {
        return values[i];
      }
      if (k == EMPTY) {
        return null; // The loop always ends, because the table is never full.
      }
    }
  }

  private Map<Integer, E> toMap() {
    Map<Integer, E> result = new TreeMap<>();
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] != EMPTY) {
        result.put(keys[i], values[i]);
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "SparseIntMap" + toMap();
  }
}
