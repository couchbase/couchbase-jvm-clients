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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static com.couchbase.client.core.util.CbCollections.listCopyOf;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Objects.requireNonNull;

enum AppTelemetryRequestType {
  KV_RETRIEVAL(ServiceType.KV,
    Duration.ofMillis(1),
    Duration.ofMillis(10),
    Duration.ofMillis(100),
    Duration.ofMillis(500),
    Duration.ofMillis(1_000),
    Duration.ofMillis(2_500)
  ),

  // Can't fix spelling, since metric name is derived from enum value name
  @SuppressWarnings("SpellCheckingInspection")
  KV_MUTATION_NONDURABLE(ServiceType.KV, KV_RETRIEVAL.buckets),

  KV_MUTATION_DURABLE(ServiceType.KV,
    Duration.ofMillis(10),
    Duration.ofMillis(100),
    Duration.ofMillis(500),
    Duration.ofSeconds(1),
    Duration.ofSeconds(2),
    Duration.ofSeconds(10)
  ),

  QUERY(ServiceType.QUERY,
    Duration.ofMillis(100),
    Duration.ofSeconds(1),
    Duration.ofSeconds(10),
    Duration.ofSeconds(30),
    Duration.ofSeconds(75)
  ),

  SEARCH(ServiceType.SEARCH, QUERY.buckets),

  ANALYTICS(ServiceType.ANALYTICS, QUERY.buckets),

  EVENTING(ServiceType.EVENTING, QUERY.buckets),

  MANAGEMENT(ServiceType.MANAGER, QUERY.buckets),
  ;

  static final List<AppTelemetryRequestType> valueList = listOf(values());

  final ServiceType service;
  final String name;
  final List<Duration> buckets;

  AppTelemetryRequestType(ServiceType service, Duration... buckets) {
    this(service, Arrays.asList(buckets));
  }

  AppTelemetryRequestType(ServiceType service, List<Duration> buckets) {
    this.service = requireNonNull(service);
    this.buckets = listCopyOf(buckets);
    this.name = "sdk_" + name().toLowerCase(Locale.ROOT);
  }
}
