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

import com.couchbase.client.core.service.ServiceType;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Collections.unmodifiableMap;

/**
 * All the histograms and counters for a specific node + bucket.
 */
class AppTelemetryMetricSet {
  private static final List<ServiceType> trackedServices = listOf(
    ServiceType.KV,
    ServiceType.QUERY,
    ServiceType.SEARCH,
    ServiceType.ANALYTICS,
    ServiceType.EVENTING,
    ServiceType.MANAGER
  );

  final Map<AppTelemetryRequestType, AppTelemetryHistogram> histograms;
  final Map<ServiceType, Map<AppTelemetryCounterType, AppTelemetryCounter>> counters;

  AppTelemetryMetricSet() {
    Map<AppTelemetryRequestType, AppTelemetryHistogram> tempHistograms = new EnumMap<>(AppTelemetryRequestType.class);
    AppTelemetryRequestType.valueList.forEach(type -> tempHistograms.put(type, new AppTelemetryHistogram(type)));
    this.histograms = unmodifiableMap(tempHistograms);

    Map<ServiceType, Map<AppTelemetryCounterType, AppTelemetryCounter>> tempCounters = new EnumMap<>(ServiceType.class);
    trackedServices.forEach(service -> tempCounters.put(service, newCounters(service)));
    this.counters = unmodifiableMap(tempCounters);
  }

  private static Map<AppTelemetryCounterType, AppTelemetryCounter> newCounters(ServiceType serviceType) {
    Map<AppTelemetryCounterType, AppTelemetryCounter> map = new EnumMap<>(AppTelemetryCounterType.class);
    AppTelemetryCounterType.valueList.forEach(resolution -> map.put(resolution, new AppTelemetryCounter(serviceType, resolution)));
    return unmodifiableMap(map);
  }

  void forEachMetric(Consumer<Reportable> action) {
    histograms.values().forEach(action);
    counters.values().forEach(it -> it.values().forEach(action));
  }
}
