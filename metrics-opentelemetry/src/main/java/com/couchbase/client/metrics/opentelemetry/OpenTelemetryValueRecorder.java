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

import com.couchbase.client.core.cnc.ValueRecorder;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;

public class OpenTelemetryValueRecorder implements ValueRecorder {

  private final DoubleHistogram valueRecorder;
  private final Attributes attributes;

  public OpenTelemetryValueRecorder(final DoubleHistogram valueRecorder, final Attributes attributes) {
    this.valueRecorder = valueRecorder;
    this.attributes = attributes;
  }

  @Override
  public void recordValue(final long value) {
    valueRecorder.record(value, attributes);
  }
}
