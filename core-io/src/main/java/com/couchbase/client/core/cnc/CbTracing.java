/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.service.ServiceType;

import java.util.EnumMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

@Stability.Internal
public class CbTracing {
  private CbTracing() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns a new span with the `db.system` attribute set to `couchbase`.
   * @param parent (nullable)
   */
  public static RequestSpan newSpan(CoreContext coreContext, String spanName, RequestSpan parent) {
    return newSpan(coreContext.environment().requestTracer(), spanName, parent);
  }

  /**
   * Returns a new span with the `db.system` attribute set to `couchbase`.
   * @param parent (nullable)
   */
  public static RequestSpan newSpan(RequestTracer tracer, String spanName, RequestSpan parent) {
    RequestSpan span = tracer.requestSpan(spanName, parent);
    span.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    return span;
  }

  private static final Map<ServiceType, String> serviceTypeToTracingId;

  static {
    Map<ServiceType, String> map = new EnumMap<>(ServiceType.class);
    map.put(ServiceType.ANALYTICS, TracingIdentifiers.SERVICE_ANALYTICS);
    map.put(ServiceType.EVENTING, TracingIdentifiers.SERVICE_EVENTING);
    map.put(ServiceType.KV, TracingIdentifiers.SERVICE_KV);
    map.put(ServiceType.MANAGER, TracingIdentifiers.SERVICE_MGMT);
    map.put(ServiceType.QUERY, TracingIdentifiers.SERVICE_QUERY);
    map.put(ServiceType.SEARCH, TracingIdentifiers.SERVICE_SEARCH);
    map.put(ServiceType.VIEWS, TracingIdentifiers.SERVICE_VIEWS);

    serviceTypeToTracingId = unmodifiableMap(map);
  }

  public static String getTracingId(ServiceType serviceType) {
    return serviceTypeToTracingId.getOrDefault(serviceType, serviceType.ident());
  }
}
