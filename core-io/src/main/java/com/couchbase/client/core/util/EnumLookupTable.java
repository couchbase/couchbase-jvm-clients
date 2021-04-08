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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Provides efficient lookup for enums whose values are associated with small integers.
 */
@Stability.Internal
public class EnumLookupTable<E extends Enum<E>> {
  private final E[] indexToValue;

  @SuppressWarnings("unchecked")
  private EnumLookupTable(Class<E> enumClass, Function<E, Integer> enumToIndex) {
    E[] enumValues = enumClass.getEnumConstants();
    int maxIndex = Arrays.stream(enumValues)
        .mapToInt(enumToIndex::apply)
        .max().orElse(0);

    // prevent accidentally wasting tons of memory
    if (maxIndex - enumValues.length > 1024) {
      throw new IllegalArgumentException("Lookup table is too sparse, would waste memory. " +
          "Consider adding an alternate implementation backed by a Map instead of an array.");
    }

    indexToValue = (E[]) Array.newInstance(enumClass, maxIndex + 1);
    for (E e : enumClass.getEnumConstants()) {
      int index = enumToIndex.apply(e);
      if (indexToValue[index] != null) {
        throw new IllegalArgumentException("Index collision:" +
            " enum values " + indexToValue[index] + " and " + e + " both have index " + index);
      }
      indexToValue[index] = e;
    }
  }

  /**
   * @param enumToIndex given an enum value, returns the integer associated with the value.
   */
  public static <E extends Enum<E>> EnumLookupTable<E> create(Class<E> enumClass, Function<E, Integer> enumToIndex) {
    return new EnumLookupTable<E>(enumClass, enumToIndex);
  }

  /**
   * Returns the enum value associated with the given integer, or the default value if not found.
   *
   * @param index the integer associated with the enum value to return
   */
  public E getOrDefault(int index, E defaultValue) {
    try {
      E value = indexToValue[index];
      return value == null ? defaultValue : value;
    } catch (IndexOutOfBoundsException e) {
      if (index < 0) {
        throw new IllegalArgumentException("index must be positive but got " + index);
      }
      return defaultValue;
    }
  }
}
