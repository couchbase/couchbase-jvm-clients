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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CompactIntMap.MAX_WASTED_ARRAY_ELEMENTS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntMapTest {

  private enum Color {
    RED,
    GREEN,
  }

  @Test
  void canLookUpEnumValues() {
    IntMap<Color> map = IntMap.from(Color.class, Color::ordinal);
    assertInstanceOf(CompactIntMap.class, map);
    assertEquals(Color.RED, map.get(Color.RED.ordinal()));
    assertEquals(Color.GREEN, map.get(Color.GREEN.ordinal()));
    assertNull(map.get(Color.values().length));
    assertThrows(IllegalArgumentException.class, () -> map.get(-1));
  }

  @Test
  void canLookUpArbitraryValues() {
    IntMap<String> map = IntMap.from(listOf("a", "bbb"), String::length);
    assertInstanceOf(CompactIntMap.class, map);
    assertEquals("a", map.get(1));
    assertEquals("bbb", map.get(3));
    assertNull(map.get(0));
    assertNull(map.get(2));
    assertThrows(IllegalArgumentException.class, () -> map.get(-1));
  }

  @Test
  void canCreateSparseMap() {
    IntMap<String> map = IntMap.from(mapOf(1_000_000, "a"));
    assertInstanceOf(SparseIntMap.class, map);
    assertNull(map.get(0));
    assertEquals("a", map.get(1_000_000));
    assertThrows(IllegalArgumentException.class, () -> map.get(-1));
  }

  @Test
  void sparsityEdgeConditions() {
    assertInstanceOf(CompactIntMap.class, IntMap.from(mapOf(MAX_WASTED_ARRAY_ELEMENTS, "a")));
    assertInstanceOf(SparseIntMap.class, IntMap.from(mapOf(MAX_WASTED_ARRAY_ELEMENTS + 1, "a")));

    // Filling in one of the elements brings us back under the sparsity threshold.
    assertInstanceOf(CompactIntMap.class, IntMap.from(mapOf(
      MAX_WASTED_ARRAY_ELEMENTS, "a",
      MAX_WASTED_ARRAY_ELEMENTS + 1, "b"
    )));

    // Adding another gap puts us over again.
    assertInstanceOf(SparseIntMap.class, IntMap.from(mapOf(
      MAX_WASTED_ARRAY_ELEMENTS, "a",
      MAX_WASTED_ARRAY_ELEMENTS + 1, "b",
      MAX_WASTED_ARRAY_ELEMENTS + 3, "c"
    )));
  }

  @Test
  void reinterpretsNegativeShortAsPositiveInt() {
    IntMap<String> map = IntMap.from(mapOf(
      0xffff, "a",
      0, "b",
      (int) Short.MAX_VALUE, "c",
      Short.MIN_VALUE & 0xffff, "d"
    ));
    assertEquals("a", map.get((short) -1));
    assertEquals("b", map.get((short) 0));
    assertEquals("c", map.get(Short.MAX_VALUE));
    assertEquals("d", map.get(Short.MIN_VALUE));
    assertThrows(IllegalArgumentException.class, () -> map.get(-1));
  }

  @Test
  void canCreateEmpty() {
    IntMap<String> map = IntMap.from(emptyList(), String::length);
    assertInstanceOf(CompactIntMap.class, map);
    assertNull(map.get(0));
    assertThrows(IllegalArgumentException.class, () -> map.get(-1));
  }

  @Test
  void emptyInstanceIsCachedSingleton() {
    assertSame(
      IntMap.from(emptyList(), String::length),
      IntMap.from(emptyMap())
    );
  }

  @Test
  void creationFailsIfKeysAreNotUnique() {
    assertThrows(
      IllegalArgumentException.class,
      () -> IntMap.from(listOf("a", "b"), String::length)
    );
  }

  @Test
  void creationFailsIfKeyIsNull() {
    assertThrows(
      IllegalArgumentException.class,
      () -> IntMap.from(listOf("a"), it -> null)
    );

    assertThrows(
      IllegalArgumentException.class,
      () -> {
        Map<Integer, String> mapWithNullKey = new HashMap<>();
        mapWithNullKey.put(null, "a");
        IntMap.from(mapWithNullKey);
      }
    );
  }

  @Test
  void creationFailsIfKeyExtractorIsNullEvenForEmptyList() {
    assertThrows(
      NullPointerException.class,
      () -> IntMap.from(emptyList(), null)
    );
  }
}
