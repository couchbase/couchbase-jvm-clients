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
import com.couchbase.client.core.annotation.UsedBy;
import com.couchbase.client.core.cnc.tracing.NoopRequestSpan;
import com.couchbase.client.core.cnc.tracing.NoopRequestTracer;
import com.couchbase.client.core.cnc.tracing.ThresholdLoggingTracer;
import com.couchbase.client.core.cnc.tracing.ThresholdRequestSpan;
import com.couchbase.client.core.service.ServiceType;

import java.util.EnumMap;
import java.util.Map;

import static com.couchbase.client.core.annotation.UsedBy.Project.SPRING_DATA_COUCHBASE;
import static java.util.Collections.unmodifiableMap;

@Stability.Internal
public class CbTracing {
  private CbTracing() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns true if the tracer is an internal one (noop or threshold) so that particular
   * optimizations can be applied.
   *
   * @param tracer the tracer to check.
   * @return true if internal, false otherwise.
   */
  public static boolean isInternalTracer(final RequestTracer tracer) {
    return tracer instanceof NoopRequestTracer || tracer instanceof ThresholdLoggingTracer;
  }

  /**
   * Returns true if the span is an internal one (noop or threshold) so that particular
   * optimizations can be applied.
   *
   * @param span the span to check.
   * @return true if internal, false otherwise.
   */
  public static boolean isInternalSpan(final RequestSpan span) {
    return span instanceof NoopRequestSpan || span instanceof ThresholdRequestSpan;
  }

  /**
   * Returns a new span with the `db.system` attribute set to `couchbase`.
   * @param parent (nullable)
   */
  @UsedBy(SPRING_DATA_COUCHBASE)
  public static RequestSpan newSpan(CoreContext coreContext, String spanName, RequestSpan parent) {
    return coreContext.coreResources().requestTracer().requestSpan(spanName, parent);
  }

  /**
   * Returns a new span with the `db.system` attribute set to `couchbase`.
   *
   * @param parent (nullable)
   * @deprecated In favor of {@link RequestTracer#requestSpan(String, RequestSpan)},
   * which now always sets the `db.system` attribute.
   */
  @Deprecated
  public static RequestSpan newSpan(RequestTracer tracer, String spanName, RequestSpan parent) {
    return tracer.requestSpan(spanName, parent);
  }

  /**
   * @param span (nullable)
   * @param attributes (nullable)
   */
  public static void setAttributes(RequestSpan span, Map<String, ?> attributes) {
    if (span == null || attributes == null) {
      return;
    }
    attributes.forEach((k, v) -> {
      if (v instanceof String) {
        span.attribute(k, (String) v);
      } else if (v instanceof Integer || v instanceof Long) {
        span.attribute(k, ((Number) v).longValue());
      } else if (v instanceof Boolean) {
        span.attribute(k, (Boolean) v);
      } else {
        span.attribute(k, String.valueOf(v));
      }
    });
  }

  private static final Map<ServiceType, String> serviceTypeToTracingId;

  static {
    Map<ServiceType, String> map = new EnumMap<>(ServiceType.class);
    map.put(ServiceType.ANALYTICS, TracingIdentifiers.SERVICE_ANALYTICS);
    map.put(ServiceType.EVENTING, TracingIdentifiers.SERVICE_EVENTING);
    map.put(ServiceType.BACKUP, TracingIdentifiers.SERVICE_BACKUP);
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
