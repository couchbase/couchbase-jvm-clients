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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.Request;

import java.util.function.Consumer;

@Stability.Internal
public interface AppTelemetryCollector {
  AppTelemetryCollector NOOP = new AppTelemetryCollector() {
    @Override
    public void recordLatency(Request<?> request) {
    }

    @Override
    public void increment(Request<?> request, AppTelemetryCounterType counterType) {
    }

    @Override
    public void reportTo(Consumer<? super CharSequence> charSink) {
    }

    @Override
    public void setPaused(boolean paused) {
    }
  };

  void recordLatency(Request<?> request);

  void increment(Request<?> request, AppTelemetryCounterType counterType);

  void reportTo(Consumer<? super CharSequence> charSink);

  void setPaused(boolean paused);
}
