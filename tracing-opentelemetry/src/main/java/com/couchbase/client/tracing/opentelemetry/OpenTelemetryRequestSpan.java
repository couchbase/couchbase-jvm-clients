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
import com.couchbase.client.core.msg.RequestContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

import java.time.Instant;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Wraps an OpenTelemetry span, ready to be passed in into options for each operation into the SDK as a parent.
 */
public class OpenTelemetryRequestSpan implements RequestSpan {

  /**
   * The OT parent span.
   */
  private final Span span;

  private OpenTelemetryRequestSpan(final Span span) {
    this.span = notNull(span, "Span");
  }

  /**
   * Wraps an OpenTelemetry span so that it can be passed in to the SDK-operation options as a parent.
   *
   * @param span the span that should act as the parent.
   * @return the created wrapped span.
   */
  public static OpenTelemetryRequestSpan wrap(final Span span) {
    return new OpenTelemetryRequestSpan(span);
  }

  /**
   * Returns the wrapped OpenTelemetry span.
   */
  public Span span() {
    return span;
  }

  @Override
  public void attribute(String key, String value) {
    span.setAttribute(key, value);
  }

  @Override
  public void attribute(String key, boolean value) {
    span.setAttribute(key, value);
  }

  @Override
  public void attribute(String key, long value) {
    span.setAttribute(key, value);
  }

  @Override
  public void event(String name, Instant timestamp) {
    span.addEvent(name, timestamp);
  }

  @Override
  public void end() {
    span.end();
  }

  @Override
  public void requestContext(final RequestContext requestContext) {
    // no need for the request context in this implementation
  }
}
