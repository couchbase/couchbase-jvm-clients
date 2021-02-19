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

package com.couchbase.client.metrics.opentelemetry;

import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.cnc.metrics.NameAndTags;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongValueRecorder;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.api.metrics.common.LabelsBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OpenTelemetryMeter implements Meter {

  private final io.opentelemetry.api.metrics.Meter otMeter;
  private final Map<NameAndTags, OpenTelemetryCounter> counters = new ConcurrentHashMap<>();
  private final Map<NameAndTags, OpenTelemetryValueRecorder> valueRecorders = new ConcurrentHashMap<>();

  public static OpenTelemetryMeter wrap(io.opentelemetry.api.metrics.Meter otMeter) {
    return new OpenTelemetryMeter(otMeter);
  }

  private OpenTelemetryMeter(io.opentelemetry.api.metrics.Meter otMeter) {
    this.otMeter = otMeter;
  }

  @Override
  public Counter counter(final String name, final Map<String, String> tags) {
    return counters.computeIfAbsent(new NameAndTags(name, tags), key -> {
      LongCounter counter = otMeter.longCounterBuilder(name).build();
      final LabelsBuilder builder = Labels.builder();
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.put(tag.getKey(), tag.getValue());
      }
      return new OpenTelemetryCounter(counter.bind(builder.build()));
    });
  }

  @Override
  public ValueRecorder valueRecorder(final String name, final Map<String, String> tags) {
    return valueRecorders.computeIfAbsent(new NameAndTags(name, tags), key -> {
      LongValueRecorder vc =  otMeter.longValueRecorderBuilder(name).build();
      final LabelsBuilder builder = Labels.builder();
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.put(tag.getKey(), tag.getValue());
      }
      return new OpenTelemetryValueRecorder(vc.bind(builder.build()));
    });
  }

}
