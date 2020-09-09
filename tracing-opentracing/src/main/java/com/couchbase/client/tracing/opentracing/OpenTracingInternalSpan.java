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

package com.couchbase.client.tracing.opentracing;

import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import com.couchbase.client.core.service.ServiceType;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;

import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_ANALYTICS;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_KV;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_QUERY;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_SEARCH;
import static com.couchbase.client.core.cnc.RequestTracer.SERVICE_IDENTIFIER_VIEW;

/**
 * The internal span which handles all the different SDK events and stores/handles the appropriate sub-spans.
 */
public class OpenTracingInternalSpan implements InternalSpan {

  private final Tracer tracer;
  private final Span span;

  private volatile RequestContext ctx;
  private volatile Span dispatchSpan;
  private volatile Span encodingSpan;

  OpenTracingInternalSpan(final Tracer tracer, final Span parent, final String operationName) {
    this.tracer = tracer;
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operationName);
    if (parent != null) {
      spanBuilder.asChildOf(parent);
    }
    this.span = spanBuilder.start();
    tracer.activateSpan(span).close();
  }

  @Override
  public void finish() {
    try (Scope scope = tracer.activateSpan(span)) {
      span.setTag(Tags.PEER_SERVICE, mapServiceType(ctx.request().serviceType()));
      String operationId = ctx.request().operationId();
      if (operationId != null) {
        span.setTag("couchbase.operation_id", operationId);
      }
      if (ctx.request() instanceof BaseKeyValueRequest) {
        span.setTag("couchbase.document_id",
                new String(((BaseKeyValueRequest) ctx.request()).key(), CharsetUtil.UTF_8));
      }
      if (ctx.clientContext() != null) {
        ctx.clientContext().forEach((key, value) ->
                span.setTag("couchbase.client_context." + key, value.toString()));
      }
      span.finish();
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
    dispatchSpan = tracer.buildSpan(RequestTracer.DISPATCH_SPAN_NAME).asChildOf(span).start();
    tracer.activateSpan(dispatchSpan).close();
  }

  @Override
  public void stopDispatch() {
    try (Scope scope = tracer.activateSpan(dispatchSpan)) {
      long serverLatency = ctx.serverLatency();
      if (serverLatency > 0) {
        dispatchSpan.setTag("peer.latency", serverLatency);
      }
      dispatchSpan.finish();
    }
  }

  @Override
  public void startPayloadEncoding() {
    encodingSpan = tracer.buildSpan(RequestTracer.PAYLOAD_ENCODING_SPAN_NAME).asChildOf(span).start();
    tracer.activateSpan(encodingSpan).close();
  }

  @Override
  public void stopPayloadEncoding() {
    try (Scope scope = tracer.activateSpan(encodingSpan)) {
      encodingSpan.finish();
    }
  }

  @Override
  public RequestSpan toRequestSpan() {
    return OpenTracingRequestSpan.wrap(tracer, span);
  }

}
