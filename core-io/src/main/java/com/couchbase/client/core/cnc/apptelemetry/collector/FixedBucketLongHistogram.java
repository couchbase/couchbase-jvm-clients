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

package com.couchbase.client.core.cnc.apptelemetry.collector;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

class FixedBucketLongHistogram {
  private final long[] bucketUpperBoundInclusive;
  private final LongAdder[] buckets;

  public FixedBucketLongHistogram(List<Long> bucketUpperBoundInclusive) {
    this.bucketUpperBoundInclusive = requireSortedAndDistinct(toPrimitiveLongArray(bucketUpperBoundInclusive));
    this.buckets = new LongAdder[bucketUpperBoundInclusive.size()];
    Arrays.setAll(buckets, i -> new LongAdder());
  }

  private static long[] toPrimitiveLongArray(Collection<Long> longs) {
    return longs.stream().mapToLong(Long::longValue).toArray();
  }

  private static long[] requireSortedAndDistinct(long[] array) {
    long[] sortedAndDistinct = Arrays.stream(array).sorted().distinct().toArray();
    if (!Arrays.equals(array, sortedAndDistinct)) {
      throw new IllegalArgumentException("Array has duplicate elements or is not sorted: " + Arrays.toString(array));
    }
    return array;
  }

  public void record(long value) {
    for (int i = 0, len = bucketUpperBoundInclusive.length; i < len; i++) {
      if (value <= bucketUpperBoundInclusive[i]) {
        buckets[i].increment();
        return;
      }
    }
  }

  public long[] report() {
    long[] result = new long[buckets.length];
    for (int i = 0, len = buckets.length; i < len; i++) {
      result[i] = buckets[i].sumThenReset();
    }
    return result;
  }
}
