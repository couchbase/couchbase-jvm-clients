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
import com.couchbase.client.core.cnc.diagnostics.GcAnalyzer;
import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GarbageCollectionsDetectedEvent extends AbstractEvent  {

  private final Map<GcAnalyzer.GcType, Histogram> histograms;

  public GarbageCollectionsDetectedEvent(Severity severity, Context context,
                                         Map<GcAnalyzer.GcType, Histogram> histograms) {
    super(severity, Category.SYSTEM, Duration.ZERO, context);
    this.histograms = histograms;
  }

  public Set<GcAnalyzer.GcType> types() {
    return histograms.keySet();
  }

  public long totalCount(GcAnalyzer.GcType type) {
    return histograms.get(type).getTotalCount();
  }

  public long valueAtPercentile(GcAnalyzer.GcType type, double p) {
    return histograms.get(type).getValueAtPercentile(p);
  }

  public long maxValue(GcAnalyzer.GcType type) {
    return histograms.get(type).getMaxValue();
  }

  public Duration collectionDuration(GcAnalyzer.GcType type) {
    Histogram histogram = histograms.get(type);
    return Duration.ofMillis(histogram.getEndTimeStamp() - histogram.getStartTimeStamp());
  }

  @Override
  public String description() {
    List<String> encoded = histograms.entrySet().stream().map(entry -> {
      Histogram h = entry.getValue();
      Map<String, Object> gc = new HashMap<>();
      gc.put("type", entry.getKey());
      gc.put("max", h.getMaxValue() + "ms");
      gc.put("p50", h.getValueAtPercentile(50.0) + "ms");
      gc.put("p90", h.getValueAtPercentile(90.0) + "ms");
      gc.put("p99", h.getValueAtPercentile(99.0) + "ms");
      gc.put("p99.9", h.getValueAtPercentile(99.9) + "ms");
      return gc.toString();
    }).collect(Collectors.toList());

    return "Detected GC pauses during the collection interval: " + encoded;
  }

}
