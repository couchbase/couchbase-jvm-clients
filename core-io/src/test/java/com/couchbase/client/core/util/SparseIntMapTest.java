/*
 * Copyright 2026 Couchbase, Inc.
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the hash table behind {@link SparseIntMap} directly, including collisions and wrap-around.
 * <p>
 * {@link IntMapTest} covers {@link IntMap#from} (including when it chooses this class)
 * and the interface's default methods.
 */
class SparseIntMapTest {

  @Test
  void singleEntry() {
    SparseIntMap<String> map = new SparseIntMap<>(mapOf(1_000_000, "a"));
    assertEquals(2, map.capacity());
    assertEquals("a", map.get(1_000_000));
    assertNull(map.get(0));
    assertNull(map.get(999_999));
    assertNull(map.get(1_000_001));
    assertNull(map.get(Integer.MAX_VALUE));
  }

  @Test
  void emptyMapFindsNothing() {
    // IntMap.from never builds an empty SparseIntMap, but the class should still handle it.
    SparseIntMap<String> map = new SparseIntMap<>(Collections.emptyMap());
    assertEquals(2, map.capacity());
    assertNull(map.get(0));
    assertNull(map.get(Integer.MAX_VALUE));
    assertEquals("SparseIntMap{}", map.toString());
  }

  @Test
  void extremeKeys() {
    Map<Integer, String> source = mapOf(
      0, "zero",
      1, "one",
      Integer.MAX_VALUE - 1, "max-1",
      Integer.MAX_VALUE, "max"
    );
    SparseIntMap<String> map = new SparseIntMap<>(source);
    source.forEach((key, value) -> assertEquals(value, map.get(key)));
    assertNull(map.get(2));
    assertNull(map.get(Integer.MAX_VALUE - 2));
  }

  @Test
  void negativeKeysThrow() {
    // -1 is the marker for an empty slot, so it must be rejected rather than matched.
    SparseIntMap<String> map = new SparseIntMap<>(mapOf(1_000_000, "a"));
    assertThrows(IllegalArgumentException.class, () -> map.get(-1));
    assertThrows(IllegalArgumentException.class, () -> map.get(-2));
    assertThrows(IllegalArgumentException.class, () -> map.get(Integer.MIN_VALUE));
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, -2, Integer.MIN_VALUE})
  void constructorRejectsNegativeKeys(int key) {
    // -1 matters most: it's the empty-slot marker, so it would otherwise vanish without an error.
    Map<Integer, String> source = new HashMap<>();
    source.put(1_000_000, "a");
    source.put(key, "negative");
    assertThrows(IllegalArgumentException.class, () -> new SparseIntMap<>(source));
  }

  @Test
  void constructorRejectsNullKey() {
    Map<Integer, String> source = new HashMap<>();
    source.put(1_000_000, "a");
    source.put(null, "null");
    assertThrows(IllegalArgumentException.class, () -> new SparseIntMap<>(source));
  }

  @Test
  void constructorRejectsNullValue() {
    Map<Integer, String> source = new HashMap<>();
    source.put(1_000_000, "a");
    source.put(2_000_000, null);
    assertThrows(IllegalArgumentException.class, () -> new SparseIntMap<>(source));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 100, 1000, 4096, 50_000})
  void findsEveryKeyAndNothingElse(int size) {
    Random random = new Random(size);
    Map<Integer, String> source = new HashMap<>();
    while (source.size() < size) {
      int key = random.nextInt(Integer.MAX_VALUE);
      source.put(key, "value-" + key);
    }

    SparseIntMap<String> map = new SparseIntMap<>(source);
    assertEquals(SparseIntMap.capacityFor(size), map.capacity());

    source.forEach((key, value) -> assertEquals(value, map.get(key)));

    for (int i = 0; i < 10_000; i++) {
      int key = random.nextInt(Integer.MAX_VALUE);
      assertEquals(source.get(key), map.get(key), "key " + key);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 1024, 1 << 16, 1 << 20, 1 << 24})
  void findsPatternedKeys(int stride) {
    // Keys that differ only in their high bits, or are consecutive, are what a naive hash (key & mask) handles worst.
    int size = Math.min(1000, Integer.MAX_VALUE / stride);
    Map<Integer, String> source = new HashMap<>();
    for (int i = 0; i < size; i++) {
      int key = 1_000_000 + i * stride;
      if (key < 0) break; // overflowed
      source.put(key, "value-" + key);
    }

    SparseIntMap<String> map = new SparseIntMap<>(source);
    source.forEach((key, value) -> assertEquals(value, map.get(key)));
    for (int key : source.keySet()) {
      if (key + 1 > 0 && !source.containsKey(key + 1)) {
        assertNull(map.get(key + 1));
      }
    }
  }

  @Test
  void consecutiveKeysAreSpreadAcrossSlots() {
    // Not needed for correctness, but guards against a hash change that would make lookups slow.
    Map<Integer, String> source = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      source.put(1_000_000 + i, "v");
    }
    SparseIntMap<String> map = new SparseIntMap<>(source);

    Set<Integer> firstSlots = new HashSet<>();
    source.keySet().forEach(key -> firstSlots.add(map.slot(key)));
    assertTrue(firstSlots.size() > source.size() * 9 / 10, "distinct first slots: " + firstSlots.size());
  }

  @Test
  void collidingKeysAreAllFound() {
    // A table for 4 entries has 8 slots. Put all 4 keys in the same first slot.
    int size = 4;
    SparseIntMap<?> shape = shapeFor(size);
    int slot = 3;
    List<Integer> colliding = keysWithFirstSlot(shape, slot, size + 1);
    List<Integer> keys = colliding.subList(0, size);
    int absentKey = colliding.get(size);

    SparseIntMap<String> map = new SparseIntMap<>(valuesFor(keys));
    assertEquals(shape.capacity(), map.capacity());
    for (int key : keys) {
      assertEquals(slot, map.slot(key));
      assertEquals("value-" + key, map.get(key));
    }

    // A missing key with the same first slot has to walk past the whole cluster to find it's absent.
    assertNull(map.get(absentKey));
  }

  @Test
  void collidingKeysAreFoundWhateverTheInsertionOrder() {
    int size = 4;
    SparseIntMap<?> shape = shapeFor(size);
    List<Integer> keys = keysWithFirstSlot(shape, 5, size);

    for (List<Integer> order : permutations(keys)) {
      SparseIntMap<String> map = new SparseIntMap<>(valuesFor(order));
      for (int key : keys) {
        assertEquals("value-" + key, map.get(key), "insertion order " + order);
      }
      assertEquals(
        "SparseIntMap" + new TreeMap<>(valuesFor(keys)),
        map.toString(),
        "insertion order " + order
      );
    }
  }

  @Test
  void probingWrapsAroundFromLastSlotToFirst() {
    // Every key starts at the last slot, so all but one of them wrap around to the start of the table.
    int size = 4;
    SparseIntMap<?> shape = shapeFor(size);
    int lastSlot = shape.capacity() - 1;
    List<Integer> colliding = keysWithFirstSlot(shape, lastSlot, size + 1);
    List<Integer> keys = colliding.subList(0, size);
    int absentKey = colliding.get(size);

    SparseIntMap<String> map = new SparseIntMap<>(valuesFor(keys));
    for (int key : keys) {
      assertEquals("value-" + key, map.get(key));
    }
    assertNull(map.get(absentKey));
  }

  @Test
  void missStopsAtEmptySlotEvenWhenNeighboursAreOccupied() {
    // Two clusters with one empty slot between them: a miss in the first cluster must stop at the gap,
    // and a key that belongs to the second cluster must still be found.
    int size = 4; // 8 slots
    SparseIntMap<?> shape = shapeFor(size);
    List<Integer> first = keysWithFirstSlot(shape, 0, 3); // slots 0, 1, 2
    List<Integer> second = keysWithFirstSlot(shape, 4, 1); // slot 4; slot 3 stays empty
    List<Integer> absent = keysWithFirstSlot(shape, 0, 4).subList(3, 4);

    List<Integer> keys = new ArrayList<>(first);
    keys.addAll(second);
    SparseIntMap<String> map = new SparseIntMap<>(valuesFor(keys));

    for (int key : keys) {
      assertEquals("value-" + key, map.get(key));
    }
    assertNull(map.get(absent.get(0)));
  }

  @Test
  void fullestTableStillFindsEverything() {
    // A power-of-two size fills the table exactly halfway, the most it ever gets.
    int size = 512;
    SparseIntMap<?> shape = shapeFor(size);
    assertEquals(size * 2, shape.capacity());

    // Pile everything into a handful of first slots, so the clusters are long and merge.
    List<Integer> keys = new ArrayList<>();
    for (int slot = 0; keys.size() < size; slot += 64) {
      keys.addAll(keysWithFirstSlot(shape, slot % shape.capacity(), Math.min(64, size - keys.size())));
    }
    SparseIntMap<String> map = new SparseIntMap<>(valuesFor(keys));

    for (int key : keys) {
      assertEquals("value-" + key, map.get(key));
    }

    Random random = new Random(0);
    Set<Integer> present = new HashSet<>(keys);
    for (int i = 0; i < 10_000; i++) {
      int key = random.nextInt(Integer.MAX_VALUE);
      if (!present.contains(key)) {
        assertNull(map.get(key));
      }
    }
  }

  @Test
  void capacityIsSmallestPowerOfTwoAtLeastTwiceTheSize() {
    assertEquals(2, SparseIntMap.capacityFor(0));
    assertEquals(2, SparseIntMap.capacityFor(1));
    assertEquals(4, SparseIntMap.capacityFor(2));
    assertEquals(8, SparseIntMap.capacityFor(3));
    assertEquals(8, SparseIntMap.capacityFor(4));
    assertEquals(16, SparseIntMap.capacityFor(5));
    assertEquals(1 << 30, SparseIntMap.capacityFor(1 << 29));

    for (int size = 1; size <= 100_000; size++) {
      int capacity = SparseIntMap.capacityFor(size);
      assertEquals(1, Integer.bitCount(capacity), "size " + size);
      assertTrue(capacity >= size * 2, "size " + size);
      assertTrue(capacity < size * 4, "size " + size);
    }
  }

  @Test
  void capacityRejectsUnsupportedSizes() {
    assertThrows(IllegalArgumentException.class, () -> SparseIntMap.capacityFor(-1));
    assertThrows(IllegalArgumentException.class, () -> SparseIntMap.capacityFor((1 << 29) + 1));
  }

  @Test
  void laterChangesToSourceMapHaveNoEffect() {
    Map<Integer, String> source = new HashMap<>();
    source.put(1_000_000, "a");
    SparseIntMap<String> map = new SparseIntMap<>(source);

    source.put(1_000_000, "changed");
    source.put(2_000_000, "added");

    assertEquals("a", map.get(1_000_000));
    assertNull(map.get(2_000_000));
  }

  @Test
  void toStringListsEntriesInKeyOrder() {
    Map<Integer, String> source = new LinkedHashMap<>();
    source.put(2_000_000, "c");
    source.put(7, "a");
    source.put(1_000_000, "b");
    assertEquals("SparseIntMap{7=a, 1000000=b, 2000000=c}", new SparseIntMap<>(source).toString());
  }

  /**
   * Returns an empty-valued map with the given number of entries. Its slot function and capacity
   * are the same as any other map with that many entries.
   */
  private static SparseIntMap<?> shapeFor(int size) {
    Map<Integer, String> source = new HashMap<>();
    for (int i = 0; i < size; i++) {
      source.put(i, "");
    }
    return new SparseIntMap<>(source);
  }

  /**
   * Returns the given number of distinct keys, all at least 1,000,000, whose first slot is the given one.
   */
  private static List<Integer> keysWithFirstSlot(
    SparseIntMap<?> shape,
    int slot,
    int count
  ) {
    List<Integer> result = new ArrayList<>();
    for (int key = 1_000_000; result.size() < count; key++) {
      if (shape.slot(key) == slot) {
        result.add(key);
      }
    }
    return result;
  }

  private static Map<Integer, String> valuesFor(List<Integer> keys) {
    Map<Integer, String> result = new LinkedHashMap<>();
    keys.forEach(key -> result.put(key, "value-" + key));
    return result;
  }

  private static List<List<Integer>> permutations(List<Integer> items) {
    if (items.isEmpty()) {
      return listOf(listOf());
    }
    List<List<Integer>> result = new ArrayList<>();
    for (int i = 0; i < items.size(); i++) {
      List<Integer> rest = new ArrayList<>(items);
      Integer first = rest.remove(i);
      for (List<Integer> permutation : permutations(rest)) {
        List<Integer> withFirst = new ArrayList<>();
        withFirst.add(first);
        withFirst.addAll(permutation);
        result.add(withFirst);
      }
    }
    return result;
  }
}
