/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.tracing.opentelemetry;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.msg.RequestContext;
import io.opentelemetry.context.Scope;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Tracer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Wraps an OpenTelemetry span, ready to be passed in into options for each operation into the SDK as a parent.
 */
public class OpenTelemetryRequestSpan implements RequestSpan {

  /**
   * The OT parent span.
   */
  private final Span span;

  private final Tracer tracer;

  private volatile RequestContext requestContext;

  private OpenTelemetryRequestSpan(final Tracer tracer, final Span span) {
    notNull(tracer, "Tracer");
    notNull(span, "Span");
    this.tracer = tracer;
    this.span = span;
  }

  /**
   * Wraps an OpenTelemetry span so that it can be passed in to the SDK-operation options as a parent.
   *
   * @param span the span that should act as the parent.
   * @return the created wrapped span.
   */
  public static OpenTelemetryRequestSpan wrap(final Tracer tracer, final Span span) {
    return new OpenTelemetryRequestSpan(tracer, span);
  }

  /**
   * Returns the OT parent span if present.
   */
  Span span() {
    return span;
  }

  @Override
  public void setAttribute(String key, String value) {
    span.setAttribute(key, value);
  }

  @Override
  public void addEvent(String name, Instant timestamp) {
    span.addEvent(name, ChronoUnit.NANOS.between(Instant.EPOCH, timestamp));
  }

  @Override
  public void end(RequestTracer tracer) {
    try (Scope scope = this.tracer.withSpan(span)) {
      span.end();
    }
  }

  @Override
  public void requestContext(final RequestContext requestContext) {
    this.requestContext = requestContext;
  }
}
