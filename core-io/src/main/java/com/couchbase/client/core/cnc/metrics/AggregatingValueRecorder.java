/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.metrics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.deps.org.HdrHistogram.Histogram;
import com.couchbase.client.core.deps.org.LatencyUtils.LatencyStats;

import java.util.Map;

/**
 * Aggregates value information in a histogram.
 */
@Stability.Volatile
public class AggregatingValueRecorder implements ValueRecorder {

  private final Map<String, String> tags;

  private final LatencyStats recorderStats = new LatencyStats();

  public AggregatingValueRecorder(final Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public void recordValue(long value) {
    recorderStats.recordLatency(value);
  }

  Histogram clearStats() {
    return recorderStats.getIntervalHistogram();
  }

  Map<String, String> tags() {
    return tags;
  }
}
