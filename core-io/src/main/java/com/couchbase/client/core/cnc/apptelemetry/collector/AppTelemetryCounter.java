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

import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.service.ServiceType;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryHistogram.formatTags;

class AppTelemetryCounter implements Reportable {
  private final String name;
  private final LongAdder count = new LongAdder();

  AppTelemetryCounter(
    ServiceType serviceType,
    AppTelemetryCounterType type
  ) {
    this.name = "sdk_" + serviceType.id() + "_" + type.name;
  }

  public void increment() {
    count.increment();
  }

  @Override
  public void reportTo(
    Consumer<? super CharSequence> charSink,
    Map<String, String> commonTags,
    long currentTimeMillis
  ) {
    long value = count.sumThenReset();
    if (value == 0) {
      // RFC says "The SDK should not send zero metric"
      return;
    }

    charSink.accept(name);
    charSink.accept(formatTags(commonTags));
    charSink.accept(" ");
    charSink.accept(Long.toString(value));
    charSink.accept(" ");
    charSink.accept(Long.toString(currentTimeMillis));
    charSink.accept("\n");
  }
}
