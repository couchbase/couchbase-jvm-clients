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

import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import com.couchbase.client.core.service.ServiceType;
import io.grpc.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Tracer;
import io.opentelemetry.trace.TracingContextUtils;

import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_ANALYTICS;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_KV;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_QUERY;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_SEARCH;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_VIEW;

/**
 * The internal span which handles all the different SDK events and stores/handles the appropriate sub-spans.
 */
public class OpenTelemetryInternalSpan implements InternalSpan {

  private final Tracer tracer;
  private final Span span;
  private volatile RequestContext ctx;
  private volatile Span dispatchSpan;
  private volatile Span encodingSpan;

  OpenTelemetryInternalSpan(final Tracer tracer, final Span parent, final String operationName) {
    this.tracer = tracer;

    Span.Builder spanBuilder = tracer.spanBuilder(operationName);
    if (parent != null) {
      spanBuilder.setParent(TracingContextUtils.withSpan(parent, Context.current()));
    } else {
      spanBuilder.setNoParent();
    }
    span = spanBuilder.startSpan();
    tracer.withSpan(span).close();
  }

  @Override
  public void finish() {
    try (Scope scope = tracer.withSpan(span)) {
      span.setAttribute("peer.service", mapServiceType(ctx.request().serviceType()));
      String operationId = ctx.request().operationId();
      if (operationId != null) {
        span.setAttribute("couchbase.operation_id", operationId);
      }
      if (ctx.request() instanceof BaseKeyValueRequest) {
        span.setAttribute("couchbase.document_id",
                new String(((BaseKeyValueRequest) ctx.request()).key(), CharsetUtil.UTF_8));
      }
      if (ctx.clientContext() != null) {
        ctx.clientContext().forEach((key, value) ->
                span.setAttribute("couchbase.client_context." + key, value.toString()));
      }
      span.end();
    }
  }

  private String mapServiceType(final ServiceType serviceType) {
    switch (serviceType) {
      case KV:
        return SERVICE_IDENTIFIER_KV;
      case QUERY:
        return SERVICE_IDENTIFIER_QUERY;
      case ANALYTICS:
        return SERVICE_IDENTIFIER_ANALYTICS;
      case VIEWS:
        return SERVICE_IDENTIFIER_VIEW;
      case SEARCH:
        return SERVICE_IDENTIFIER_SEARCH;
      default:
        return null;
    }
  }

  @Override
  public void requestContext(RequestContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public RequestContext requestContext() {
    return ctx;
  }

  @Override
  public void startDispatch() {
    dispatchSpan = tracer.spanBuilder(RequestTracer.DISPATCH_SPAN_NAME).setParent(TracingContextUtils.withSpan(span, Context.current())).startSpan();
    tracer.withSpan(dispatchSpan).close();
  }

  @Override
  public void stopDispatch() {
    try (Scope scope = tracer.withSpan(dispatchSpan)) {
      long serverLatency = ctx.serverLatency();
      if (serverLatency > 0) {
        dispatchSpan.setAttribute("peer.latency", serverLatency);
      }
      dispatchSpan.end();
    }
  }

  @Override
  public void startPayloadEncoding() {
    encodingSpan = tracer.spanBuilder(RequestTracer.PAYLOAD_ENCODING_SPAN_NAME).setParent(TracingContextUtils.withSpan(span, Context.current())).startSpan();
    tracer.withSpan(encodingSpan).close();
  }

  @Override
  public void stopPayloadEncoding() {
    try (Scope scope = tracer.withSpan(encodingSpan)) {
      encodingSpan.end();
    }
  }

  @Override
  public RequestSpan toRequestSpan() {
    return OpenTelemetryRequestSpan.wrap(tracer, span);
  }
}
