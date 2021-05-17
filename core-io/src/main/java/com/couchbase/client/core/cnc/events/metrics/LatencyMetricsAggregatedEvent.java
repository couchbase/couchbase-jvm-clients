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

package com.couchbase.client.core.cnc.events.metrics;

import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.json.Mapper;

import java.time.Duration;
import java.util.Map;

/**
 * This event is emitted every configured aggregating meter interval.
 */
public class LatencyMetricsAggregatedEvent extends AbstractEvent {

  private final Map<String, Object> metrics;

  public LatencyMetricsAggregatedEvent(final Duration duration, final Map<String, Object> metrics) {
    super(Severity.INFO, Category.METRICS, duration, null);
    this.metrics = metrics;
  }

  @Override
  public String description() {
    return "Aggregated Latency Metrics: " + Mapper.encodeAsString(metrics);
  }
}
