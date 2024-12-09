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

import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public enum AppTelemetryCounterType {
  TIMED_OUT("r_timedout"),
  CANCELLED("r_canceled"),
  TOTAL("r_total"),
  ;

  final String name;

  AppTelemetryCounterType(String name) {
    this.name = requireNonNull(name);
  }

  static final List<AppTelemetryCounterType> valueList = listOf(values());
}
