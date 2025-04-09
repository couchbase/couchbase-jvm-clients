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

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.unmodifiableList;

class BackoffCalculator {
  private final long maxDelayMillis;
  private final List<Long> delayMillisLookupTable;

  BackoffCalculator(Duration initialDelay, Duration maxDelay) {
    final long initialDelayMillis = Math.max(1, initialDelay.toMillis());
    this.maxDelayMillis = Math.max(1, maxDelay.toMillis());
    if (maxDelayMillis < initialDelayMillis) {
      throw new IllegalArgumentException("maxDelay must be <= initialDelay");
    }

    // Precompute delays. Radically faster than calling Math.pow() every time,
    // simpler than twiddling bits, and doesn't rely on Guava's saturated math.
    // Resulting list always has fewer than 64 elements. In practice, it has ~10.
    List<Long> mutableDelays = new ArrayList<>();
    long d = initialDelayMillis;
    try {
      do {
        mutableDelays.add(d);
        d = Math.multiplyExact(d, 2L);
      } while (d < maxDelayMillis);
    } catch (ArithmeticException done) {
      // overflow is definitely greater than maxDelay
    }
    this.delayMillisLookupTable = unmodifiableList(mutableDelays);
  }

  Duration delayForAttempt(long attempt) {
    final long millis = attempt < delayMillisLookupTable.size() ? delayMillisLookupTable.get((int) attempt) : maxDelayMillis;
    long jitteredMillis = (long) (millis * ThreadLocalRandom.current().nextDouble());
    return Duration.ofMillis(jitteredMillis);
  }
}
