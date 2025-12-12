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
import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.ValueRecorder;

/**
 * An enhanced internal version of the {@link com.couchbase.client.core.cnc.Meter} interface to more
 * easily support having multiple concurrent versions of each metric.
 */
@Stability.Internal
public interface MeterProviderImpl {
  Counter counter(CounterName metric, ResponseMetricIdentifier rmi);

  ValueRecorder valueRecorder(ValueRecorderName metric, ResponseMetricIdentifier rmi);
}
