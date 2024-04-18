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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"SuspiciousMethodCalls", "ConstantConditions", "UseBulkOperation", "SlowAbstractSetRemoveAll", "ResultOfMethodCallIgnored"})
class AtomicEnumSetTest {

  private enum Color {RED, GREEN, BLUE}

  @Test
  void noneOf() {
    AtomicEnumSet<Color> set = AtomicEnumSet.noneOf(Color.class);
    assertEquals(set, EnumSet.noneOf(Color.class));
    assertNotEquals(set, EnumSet.allOf(Color.class));

    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
  }

  @Test
  void allOf() {
    AtomicEnumSet<Color> set = AtomicEnumSet.allOf(Color.class);
    assertEquals(set, EnumSet.allOf(Color.class));
    assertNotEquals(set, EnumSet.noneOf(Color.class));

    assertEquals(3, set.size());
    assertFalse(set.isEmpty());
  }

  @Test
  void contains() {
    AtomicEnumSet<Color> set = AtomicEnumSet.allOf(Color.class);

    assertTrue(set.contains(Color.RED));
    assertTrue(set.contains(Color.GREEN));
    assertTrue(set.contains(Color.BLUE));

    set.remove(Color.GREEN);
    assertFalse(set.contains(Color.GREEN));

    assertFalse(AtomicEnumSet.noneOf(Color.class).contains(Color.GREEN));

    assertFalse(set.contains(null));
    assertFalse(EnumSet.noneOf(Color.class).contains(null));

    assertFalse(set.contains("foo"));
    assertFalse(EnumSet.noneOf(Color.class).contains("foo"));
  }

  @Test
  void containsAll() {
    AtomicEnumSet<Color> set = AtomicEnumSet.allOf(Color.class);
    assertTrue(set.containsAll(EnumSet.allOf(Color.class)));
    assertTrue(set.containsAll(setOf(Color.RED, Color.BLUE)));
    assertTrue(set.containsAll(emptySet()));

    assertThrows(NullPointerException.class, () -> set.containsAll(null));
    assertThrows(NullPointerException.class, () -> EnumSet.noneOf(Color.class).containsAll(null));

    List<?> list = new ArrayList<>(Arrays.asList(null, "foo", Color.RED));
    assertFalse(EnumSet.allOf(Color.class).containsAll(list));
    assertFalse(AtomicEnumSet.allOf(Color.class).containsAll(list));
  }

  @Test
  void add() {
    AtomicEnumSet<Color> set = AtomicEnumSet.noneOf(Color.class);

    for (Color c : Color.values()) {
      assertFalse(set.contains(c));
      assertTrue(set.add(c));
      assertTrue(set.contains(c));
      assertFalse(set.add(c));
      assertTrue(set.contains(c));
    }

    assertEquals(EnumSet.allOf(Color.class), set);

    assertThrows(NullPointerException.class, () -> set.add(null));
  }

  @Test
  void addAll() {
    AtomicEnumSet<Color> set = AtomicEnumSet.noneOf(Color.class);
    assertFalse(set.addAll(emptySet()));

    assertThrows(NullPointerException.class, () -> set.addAll(null));

    List<Color> hasNullItem = new ArrayList<>();
    hasNullItem.add(Color.RED);
    hasNullItem.add(null);
    assertThrows(NullPointerException.class, () -> set.addAll(hasNullItem));
    assertEquals(emptySet(), set); // should have failed atomically and not added RED

    assertThrows(NullPointerException.class, () -> EnumSet.noneOf(Color.class).addAll(hasNullItem));
    // EnumSet doesn't fail fast, but that's okay.
    //assertEquals(emptySet(), enumSet); // should have failed atomically and not added RED



    assertTrue(set.addAll(listOf(Color.GREEN, Color.BLUE)));
    assertTrue(set.addAll(listOf(Color.RED, Color.GREEN)));
    assertFalse(set.addAll(listOf(Color.BLUE)));

    assertEquals(EnumSet.allOf(Color.class), set);
  }

  @Test
  void remove() {
    AtomicEnumSet<Color> set = AtomicEnumSet.noneOf(Color.class);
    assertFalse(set.remove(Color.RED));
    set.add(Color.RED);
    assertEquals(setOf(Color.RED), set);
    assertTrue(set.remove(Color.RED));
    assertEquals(setOf(), set);

    assertFalse(set.remove("invalid"));
    assertFalse(EnumSet.noneOf(Color.class).remove("invalid"));

    assertFalse(set.remove(null));
    assertFalse(EnumSet.noneOf(Color.class).remove(null));
  }

  @Test
  void removeAll() {
    AtomicEnumSet<Color> set = AtomicEnumSet.allOf(Color.class);
    List<?> list = new ArrayList<>(Arrays.asList(null, "foo", Color.RED));

    assertTrue(EnumSet.allOf(Color.class).removeAll(list));
    assertTrue(set.removeAll(list));

    assertEquals(setOf(Color.GREEN, Color.BLUE), set);

    assertThrows(NullPointerException.class, () -> set.removeAll(null));
    assertThrows(NullPointerException.class, () -> EnumSet.noneOf(Color.class).removeAll(null));
  }

  @Test
  void retainAll() {
    AtomicEnumSet<Color> set = AtomicEnumSet.allOf(Color.class);
    List<?> list = new ArrayList<>(Arrays.asList(null, "foo", Color.RED));

    assertTrue(EnumSet.allOf(Color.class).retainAll(list));
    assertTrue(set.retainAll(list));

    assertEquals(setOf(Color.RED), set);

    assertThrows(NullPointerException.class, () -> set.retainAll(null));
    assertThrows(NullPointerException.class, () -> EnumSet.noneOf(Color.class).retainAll(null));
  }

  @Test
  void iterator() {
    List<Color> results = new ArrayList<>();
    for (Color c : AtomicEnumSet.allOf(Color.class)) {
      results.add(c);
    }
    assertEquals(listOf(Color.RED, Color.GREEN, Color.BLUE), results);

    assertFalse(AtomicEnumSet.noneOf(Color.class).iterator().hasNext());
  }

  @Test
  void size() {
    AtomicEnumSet<Color> set = AtomicEnumSet.noneOf(Color.class);
    assertEquals(0, set.size());
    //noinspection OverwrittenKey
    set.add(Color.RED);
    //noinspection OverwrittenKey
    set.add(Color.RED);
    assertEquals(1, set.size());
    set.add(Color.GREEN);
    assertEquals(2, set.size());
    set.remove(Color.RED);
    assertEquals(1, set.size());
  }

  @Test
  void isEmpty() {
    AtomicEnumSet<Color> set = AtomicEnumSet.noneOf(Color.class);
    assertTrue(set.isEmpty());
    set.add(Color.RED);
    assertFalse(set.isEmpty());
  }

  @Test
  void clear() {
    AtomicEnumSet<Color> set = AtomicEnumSet.allOf(Color.class);
    assertFalse(set.isEmpty());
    set.clear();
    assertTrue(set.isEmpty());
  }
}
