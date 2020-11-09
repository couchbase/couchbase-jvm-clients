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

package com.couchbase.client.tracing.opentelemetry;

import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.ValueRecorder;
import io.opentelemetry.common.Labels;
import io.opentelemetry.metrics.LongCounter;
import io.opentelemetry.metrics.LongValueRecorder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OpenTelemetryMeter implements Meter {

  private final io.opentelemetry.metrics.Meter otMeter;
  private final Map<String, OpenTelemetryCounter> counters = new ConcurrentHashMap<>();
  private final Map<String, OpenTelemetryValueRecorder> valueRecorders = new ConcurrentHashMap<>();

  public static OpenTelemetryMeter wrap(io.opentelemetry.metrics.Meter otMeter) {
    return new OpenTelemetryMeter(otMeter);
  }

  private OpenTelemetryMeter(io.opentelemetry.metrics.Meter otMeter) {
    this.otMeter = otMeter;
  }

  @Override
  public Counter counter(final String name, final Map<String, String> tags) {
    return counters.computeIfAbsent(name, key -> {
      LongCounter counter = otMeter.longCounterBuilder(key).build();
      final Labels.Builder builder = Labels.newBuilder();
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.setLabel(tag.getKey(), tag.getValue());
      }
      return new OpenTelemetryCounter(counter.bind(builder.build()));
    });
  }

  @Override
  public ValueRecorder valueRecorder(final String name, final Map<String, String> tags) {
    return valueRecorders.computeIfAbsent(name, key -> {
      LongValueRecorder vc =  otMeter.longValueRecorderBuilder(name).build();
      final Labels.Builder builder = Labels.newBuilder();
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.setLabel(tag.getKey(), tag.getValue());
      }
      return new OpenTelemetryValueRecorder(vc.bind(builder.build()));
    });
  }

}
