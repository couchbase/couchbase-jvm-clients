/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.json.stream;

import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.json.stream.GrowthStrategy.THRESHOLD;
import static com.couchbase.client.core.json.stream.GrowthStrategy.adjustSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class GrowthStrategyTest {
  @Test
  void canGrowToMaxValue() {
    check(0, Integer.MAX_VALUE, Integer.MAX_VALUE);
    check(1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE);
    check(Integer.MAX_VALUE - 1, 1, Integer.MAX_VALUE);
  }

  @Test
  void growPastMaxValueIsOutOfBounds() {
    assertThrows(IndexOutOfBoundsException.class, () -> adjustSize(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> adjustSize(Integer.MIN_VALUE));
  }

  @Test
  void exponential() {
    check(0, 1, 1);
    check(0, 2, 2);
    check(1, 1, 2);
    check(2, 1, 4);
    check(2, 2, 4);
    check(2, 3, 8);
    check(8, 8, 16);
    check(8, 9, 32);
    check(THRESHOLD / 2, 1, THRESHOLD);
  }

  @Test
  void linear() {
    check(0, THRESHOLD, THRESHOLD);
    check(1, THRESHOLD - 1, THRESHOLD);
    check(0, THRESHOLD - 1, THRESHOLD);
    check(1, THRESHOLD, THRESHOLD * 2);
    check(0, THRESHOLD + 1, THRESHOLD * 2);
    check(THRESHOLD, 2 * THRESHOLD + 1, THRESHOLD * 4);
  }

  private static void check(int currentSize, int additionalRequired, int expectedNewSize) {
    int newSize = adjustSize(currentSize + additionalRequired);
    assertValid(currentSize, newSize);
    assertEquals(expectedNewSize, newSize);
  }

  @Test
  void incrementingByOneYieldsValidSize() {
    for (int i = 0; i < 16 * THRESHOLD; i++) {
      int newSize = adjustSize(i + 1);
      assertValid(i, newSize);
    }

    // speed up the test by skipping one or two values in the middle :-)

    for (int i = Integer.MAX_VALUE - 16 * THRESHOLD; i < Integer.MAX_VALUE; i++) {
      int newSize = adjustSize(i + 1);
      assertValid(i, newSize);
    }
  }

  /**
   * Verifies the new size is either a power of 2 under the exponential threshold, or a multiple of the linear chunk size.
   */
  private static void assertValid(int oldSize, int newSize) {
    if (oldSize >= newSize) fail("New size " + newSize + " is not > old size " + oldSize);
    if (newSize < 0) fail("Negative size: " + newSize);
    if (newSize <= THRESHOLD) {
      if (Integer.bitCount(newSize) != 1) fail("Under threshold, but not power of 2: " + newSize);
      return;
    }
    if (newSize == Integer.MAX_VALUE) return;
    if (newSize % THRESHOLD != 0) fail("Over threshold, but not multiple of threshold: " + newSize);
  }
}
