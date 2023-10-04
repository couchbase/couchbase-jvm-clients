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
import com.couchbase.client.core.deps.io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.TracerException;
import com.couchbase.client.core.protostellar.GrpcAwareRequestTracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.net.URL;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Wraps the OpenTelemetry tracer so it is suitable to be passed in into the couchbase environment and picked up
 * by the rest of the SDK as a result.
 */
public class OpenTelemetryRequestTracer implements RequestTracer, GrpcAwareRequestTracer {

  public static final String INSTRUMENTATION_NAME = "com.couchbase.client.jvm";

  private static final Map<String, Attributes> MANIFEST_INFOS = new ConcurrentHashMap<>();

  static {
    try {
      Enumeration<URL> resources = CoreEnvironment.class.getClassLoader().getResources(JarFile.MANIFEST_NAME);
      while (resources.hasMoreElements()) {
        URL manifestUrl = resources.nextElement();
        if (manifestUrl == null) {
          continue;
        }
        Manifest manifest = new Manifest(manifestUrl.openStream());
        if (manifest.getEntries() == null) {
          continue;
        }
        for (Map.Entry<String, Attributes> entry : manifest.getEntries().entrySet()) {
          if (entry.getKey().startsWith("couchbase-")) {
            MANIFEST_INFOS.put(entry.getKey(), entry.getValue());
          }
        }
      }
    } catch (Exception e) {
      // Ignored on purpose.
    }
  }

  /**
   * Holds the actual OTel tracer.
   */
  private final Tracer tracer;
  private final @Nullable OpenTelemetry openTelemetry;

  /**
   * Wraps OpenTelemetry and returns a datatype that can be passed into the requestTracer method of the
   * environment.
   *
   * @param openTelemetry the OpenTelemetry instance to wrap.
   * @return the wrapped OpenTelemetry ready to be passed in.
   */
  public static OpenTelemetryRequestTracer wrap(final OpenTelemetry openTelemetry) {
    return new OpenTelemetryRequestTracer(openTelemetry.getTracerProvider(), openTelemetry);
  }

  /**
   * Wraps OpenTelemetry and returns a datatype that can be passed into the requestTracer method of the
   * environment.
   *
   * @param tracerProvider the OpenTelemetry TracerProvider instance to wrap.
   * @return the wrapped OpenTelemetry ready to be passed in.
   */
  public static OpenTelemetryRequestTracer wrap(final TracerProvider tracerProvider) {
    return new OpenTelemetryRequestTracer(tracerProvider, null);
  }

  private OpenTelemetryRequestTracer(TracerProvider tracerProvider, @Nullable OpenTelemetry openTelemetry) {
    String version = null;
    try {
      version = MANIFEST_INFOS.get("couchbase-java-tracing-opentelemetry").getValue("Impl-Version");
    } catch (Exception ex) {
      // ignored on purpose
    }

    this.tracer = version != null
      ? tracerProvider.get(INSTRUMENTATION_NAME, version)
      : tracerProvider.get(INSTRUMENTATION_NAME);
    this.openTelemetry = openTelemetry;
  }

  private Span castSpan(final RequestSpan requestSpan) {
    if (requestSpan == null) {
      return null;
    }

    if (requestSpan instanceof OpenTelemetryRequestSpan) {
      return ((OpenTelemetryRequestSpan) requestSpan).span();
    } else {
      throw new IllegalArgumentException("RequestSpan must be of type OpenTelemetryRequestSpan");
    }
  }

  /**
   * Returns the inner OpenTelemetry tracer.
   */
  public Tracer tracer() {
    return tracer;
  }

  @Override
  public RequestSpan requestSpan(String operationName, RequestSpan parent) {
    try {
      SpanBuilder spanBuilder = tracer.spanBuilder(operationName)
              // Per https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/database.md
              // Span kind: MUST always be CLIENT.
              .setSpanKind(SpanKind.CLIENT);
      Context parentContext = Context.current();
      if (parent != null) {
        parentContext = parentContext.with(castSpan(parent));
      }
      Span span = spanBuilder.setParent(parentContext).startSpan();
      return OpenTelemetryRequestSpan.wrap(span);
    } catch (Exception ex) {
      throw new TracerException("Failed to create OpenTelemetryRequestSpan", ex);
    }
  }

  @Override
  public Mono<Void> start() {
    return Mono.empty(); // Tracer is not started by us
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return Mono.empty(); // Tracer should not be stopped by us
  }

  @Override
  public void registerGrpc(com.couchbase.client.core.deps.io.grpc.ManagedChannelBuilder<?> builder) {
    if (openTelemetry != null) {
      com.couchbase.client.core.deps.io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry grpcTelemetry =
        GrpcTelemetry.create(openTelemetry);
      com.couchbase.client.core.deps.io.grpc.ClientInterceptor interceptor = grpcTelemetry.newClientInterceptor();

      builder.intercept(interceptor);
    }
  }

  @Override
  public AutoCloseable activateSpan(RequestSpan span) {
    return castSpan(span).makeCurrent();
  }
}
