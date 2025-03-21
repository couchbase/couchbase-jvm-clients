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

class GrowthStrategy {
  private GrowthStrategy() {
  }

  // Grow exponentially up to this threshold, then linearly in increments of the threshold.
  static final int THRESHOLD = 4 * 1024 * 1024;

  static int adjustSize(int minRequired) {
    if (minRequired < 0) throw new IndexOutOfBoundsException("Cannot grow beyond " + Integer.MAX_VALUE);

    int newSize = minRequired < THRESHOLD
      ? ceilPowerOfTwo(minRequired)
      : divideRoundUp(minRequired, THRESHOLD) * THRESHOLD;

    return newSize < 0
      ? Integer.MAX_VALUE // overflow, but we know it fits, so cap it here.
      : newSize;
  }

  /**
   * Returns the smallest (closest to negative infinity) positive power of 2
   * that is greater than or equal to the given value.
   */
  private static int ceilPowerOfTwo(int x) {
    int result = 0x80000000 >>> (Integer.numberOfLeadingZeros(x - 1) - 1);
    if (result < 0) throw new ArithmeticException("Next positive power of 2 for value " + x + " is greater than Integer.MAX_VALUE");
    return result;
  }

  private static int divideRoundUp(int numerator, int denominator) {
    return (numerator + denominator - 1) / denominator;
  }
}
