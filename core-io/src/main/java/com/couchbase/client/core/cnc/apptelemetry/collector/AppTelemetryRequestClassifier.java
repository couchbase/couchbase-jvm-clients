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

import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetMetaRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.ReplicaGetRequest;
import com.couchbase.client.core.msg.kv.ReplicaSubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SyncDurabilityRequest;
import com.couchbase.client.core.msg.kv.TouchRequest;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.listOf;

class AppTelemetryRequestClassifier {
  private AppTelemetryRequestClassifier() {
  }

  private static final Map<Class<?>, AppTelemetryRequestType> appTelemetryTypes;

  static {
    List<Class<?>> retrievals = listOf(
      GetRequest.class,
      ReplicaGetRequest.class,
      SubdocGetRequest.class,
      ReplicaSubdocGetRequest.class,
      GetMetaRequest.class
    );

    // KV mutations that do not support durable writes
    List<Class<?>> nonDurableMutations = listOf(
      TouchRequest.class,
      GetAndTouchRequest.class,
      GetAndLockRequest.class,
      UnlockRequest.class
    );

    // All requests that _might_ be durable (Insert, Upsert, etc.) extend SyncDurabilityRequest.
    // They are classified dynamically at runtime based on the requested durability level,
    // so not included in the quick lookup maps defined above.

    Map<Class<?>, AppTelemetryRequestType> map = new HashMap<>();
    retrievals.forEach(it -> map.put(it, AppTelemetryRequestType.KV_RETRIEVAL));
    nonDurableMutations.forEach(it -> map.put(it, AppTelemetryRequestType.KV_MUTATION_NONDURABLE));

    map.put(QueryRequest.class, AppTelemetryRequestType.QUERY);
    map.put(ServerSearchRequest.class, AppTelemetryRequestType.SEARCH);
    map.put(AnalyticsRequest.class, AppTelemetryRequestType.ANALYTICS);

    appTelemetryTypes = Collections.unmodifiableMap(map);
  }

  static @Nullable AppTelemetryRequestType classify(Request<?> request) {
    AppTelemetryRequestType type = appTelemetryTypes.get(request.getClass());
    if (type != null) {
      return type;
    }

    if (request instanceof SyncDurabilityRequest) {
      // CAVEAT: A request we classify as non-durable might actually have been durable
      // if the bucket has a minimum durability level.
      return ((SyncDurabilityRequest) request).durabilityLevel().isPresent()
        ? AppTelemetryRequestType.KV_MUTATION_DURABLE
        : AppTelemetryRequestType.KV_MUTATION_NONDURABLE;
    }

    if (request instanceof CoreHttpRequest) {
      switch (request.serviceType()) {
        case EVENTING:
            return AppTelemetryRequestType.EVENTING;
        case MANAGER:
            return AppTelemetryRequestType.MANAGEMENT;
        default:
            return null;
      }
    }

    return null;
  }
}
