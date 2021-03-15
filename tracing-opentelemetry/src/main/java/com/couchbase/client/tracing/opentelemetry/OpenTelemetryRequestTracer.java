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
import com.couchbase.client.core.env.CoreEnvironment;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import reactor.core.publisher.Mono;

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
public class OpenTelemetryRequestTracer implements RequestTracer {

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

  /**
   * Wraps OpenTelemetry and returns a datatype that can be passed into the requestTracer method of the
   * environment.
   *
   * @param openTelemetry the OpenTelemetry instance to wrap.
   * @return the wrapped OpenTelemetry ready to be passed in.
   */
  public static OpenTelemetryRequestTracer wrap(final OpenTelemetry openTelemetry) {
    return wrap(openTelemetry.getTracerProvider());
  }

  /**
   * Wraps OpenTelemetry and returns a datatype that can be passed into the requestTracer method of the
   * environment.
   *
   * @param tracerProvider the OpenTelemetry TracerProvider instance to wrap.
   * @return the wrapped OpenTelemetry ready to be passed in.
   */
  public static OpenTelemetryRequestTracer wrap(final TracerProvider tracerProvider) {
    return new OpenTelemetryRequestTracer(tracerProvider);
  }

  private OpenTelemetryRequestTracer(TracerProvider tracerProvider) {
    String version = null;
    try {
      version = MANIFEST_INFOS.get("couchbase-java-tracing-opentelemetry").getValue("Impl-Version");
    } catch (Exception ex) {
      // ignored on purpose
    }

    this.tracer = version != null
      ? tracerProvider.get(INSTRUMENTATION_NAME, version)
      : tracerProvider.get(INSTRUMENTATION_NAME);
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
    SpanBuilder spanBuilder = tracer.spanBuilder(operationName);
    Context parentContext = Context.current();
    if (parent != null) {
      parentContext = parentContext.with(castSpan(parent));
    }
    Span span = spanBuilder.setParent(parentContext).startSpan();
    return OpenTelemetryRequestSpan.wrap(span);
  }

  @Override
  public Mono<Void> start() {
    return Mono.empty(); // Tracer is not started by us
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return Mono.empty(); // Tracer should not be stopped by us
  }

}
