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

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

class AppTelemetryHistogram implements Reportable {
  private final String name;
  private final FixedBucketLongHistogram histogram;
  private final List<String> bucketNames;
  private final LongAdder totalMicros = new LongAdder(); // stored as micros to stave off overflow

  AppTelemetryHistogram(AppTelemetryRequestType type) {
    this(type.name, type.buckets);
  }

  private AppTelemetryHistogram(
    String name,
    List<Duration> bucketUpperBoundInclusive
  ) {
    List<Long> bucketUpperBoundInclusiveNanos = transform(bucketUpperBoundInclusive, Duration::toNanos);
    bucketUpperBoundInclusiveNanos.add(Long.MAX_VALUE);

    this.name = requireNonNull(name);
    this.bucketNames = formatBucketNames(bucketUpperBoundInclusiveNanos);
    this.histogram = new FixedBucketLongHistogram(bucketUpperBoundInclusiveNanos);
  }

  void record(long nanos) {
    histogram.record(nanos);
    totalMicros.add(NANOSECONDS.toMicros(nanos));
  }

  @Override
  public void reportTo(
    Consumer<? super CharSequence> charSink,
    Map<String, String> commonTags,
    long currentTimeMillisIgnored
  ) {
    long[] buckets = histogram.report();

    // Doesn't need to be atomic with respect to the histogram report; close is good enough for metrics.
    long totalMillis = MICROSECONDS.toMillis(totalMicros.sumThenReset());

    if (Arrays.stream(buckets).allMatch(it -> it == 0)) {
      // RFC says "The SDK should not send zero metric".
      // Let's consider a completely empty histogram a "zero metric".
      return;
    }

    cumulatize(buckets);

    String prefix = name + "_duration_milliseconds_";

    for (int i = 0; i < buckets.length; i++) {
      String bucketName = bucketNames.get(i);
      long bucketValue = buckets[i];
      // RFC says "SDK must send histogram buckets even if it has zero hits", so don't skip empty buckets.

      Map<String, @Nullable String> tags = new LinkedHashMap<>();
      tags.put("le", bucketName);
      tags.putAll(commonTags);
      charSink.accept(prefix + "bucket" + formatTags(tags) + " " + bucketValue + "\n");
    }

    charSink.accept(prefix + "sum" + formatTags(commonTags) + " " + totalMillis + "\n");
    charSink.accept(prefix + "count" + formatTags(commonTags) + " " + buckets[buckets.length - 1] + "\n");
  }

  static String formatTags(Map<String, @Nullable String> tags) {
    return tags.entrySet().stream()
      .filter(entry -> entry.getValue() != null) // bucket might be null
      .map(entry -> entry.getKey() + "=\"" + entry.getValue() + "\"")
      .collect(joining(",", "{", "}"));
  }

  private static List<String> formatBucketNames(List<Long> nanos) {
    return transform(nanos, it -> it == Long.MAX_VALUE ? "+Inf" : Long.toString(NANOSECONDS.toMillis(it)));
  }

  /**
   * Make the buckets cumulative, meaning each bucket
   * also contains the counts from all lower-indexed buckets.
   * <p>
   * Prometheus is weird like that.
   */
  private static void cumulatize(long[] values) {
    for (int i = 1, len = values.length; i < len; i++) {
      values[i] += values[i - 1];
    }
  }

}
