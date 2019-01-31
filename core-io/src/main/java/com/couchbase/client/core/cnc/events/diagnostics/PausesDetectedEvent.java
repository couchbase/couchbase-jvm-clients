/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.cnc.events.diagnostics;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.Context;
import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * When the analyzer detects a JVM pause, an event will be raised.
 */
public class PausesDetectedEvent extends AbstractEvent {

  private final Histogram histogram;

  public PausesDetectedEvent(Severity severity, Context context, Histogram histogram) {
    super(severity, Category.SYSTEM, Duration.ZERO, context);
    this.histogram = histogram;
  }

  public long totalCount() {
    return histogram.getTotalCount();
  }

  public long valueAtPercentile(double p) {
    return histogram.getValueAtPercentile(p);
  }

  public long maxValue() {
    return histogram.getMaxValue();
  }

  public Duration collectionDuration() {
    return Duration.ofMillis(histogram.getEndTimeStamp() - histogram.getStartTimeStamp());
  }

  @Override
  public String description() {
    List<String> collected = Stream
      .of(50.0, 90.0, 99.0, 99.9)
      .map(p -> p + ": " + valueAtPercentile(p) + "ms")
      .collect(Collectors.toList());

    return "Detected JVM pauses during the collection interval. Total Count: "
      + totalCount() + ", Max: " + maxValue() + "ms, Percentiles: " + collected
      + ", Collection Duration: " + collectionDuration();
  }
}
