/*
 * Copyright (c) 2025 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.tracing.ObservabilitySemanticConvention;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The user's choice of {@link ObservabilitySemanticConvention}(s) means they may want
 * up to two metrics output for each metric we produce (v0 and/or v1).
 * <p>
 * This class abstracts over that, created {@link WrappedValueRecorder} and
 * {@link WrappedCounter} objects that can contain one or more underlying objects.
 */
@Stability.Internal
public class AbstractMeter {
  private final MeterProviderImpl v0;
  private final MeterProviderImpl v1;
  private final boolean useV0;
  private final boolean useV1;
  // The original Meter, which could be provided from the user.  If not it is likely a LoggingMeter.
  private final Meter originalMeter;

  public AbstractMeter(Meter originalMeter, List<ObservabilitySemanticConvention> conventions) {
    requireNonNull(conventions);
    this.originalMeter = requireNonNull(originalMeter);
    if (conventions.isEmpty()) {
      this.useV0 = true;
      this.useV1 = false;
    } else if (conventions.contains(ObservabilitySemanticConvention.DATABASE_DUP)) {
      this.useV0 = true;
      this.useV1 = true;
    } else if (conventions.contains(ObservabilitySemanticConvention.DATABASE)) {
      this.useV0 = false;
      this.useV1 = true;
    } else {
      throw new IllegalArgumentException("Unknown observability convention: " + conventions);
    }
    this.v0 = new MeterProviderImplV0(originalMeter);
    this.v1 = new MeterProviderImplV1(originalMeter);
  }

  public WrappedCounter counter(CounterName metric, ResponseMetricIdentifier rmi) {
    WrappedCounter counters = new WrappedCounter();
    if (useV0) {
      counters.add(v0.counter(metric, rmi));
    }
    if (useV1) {
      counters.add(v1.counter(metric, rmi));
    }
    return counters;
  }

  public WrappedValueRecorder valueRecorder(ValueRecorderName metric, ResponseMetricIdentifier rmi) {
    WrappedValueRecorder valueRecorders = new WrappedValueRecorder();
    if (useV0) {
      valueRecorders.add(v0.valueRecorder(metric, rmi));
    }
    if (useV1) {
      valueRecorders.add(v1.valueRecorder(metric, rmi));
    }
    return valueRecorders;
  }

  public boolean isDefaultLoggingMeter() {
    return originalMeter instanceof LoggingMeter;
  }

  public boolean isNoopMeter() {
    return originalMeter instanceof NoopMeter;
  }
}
