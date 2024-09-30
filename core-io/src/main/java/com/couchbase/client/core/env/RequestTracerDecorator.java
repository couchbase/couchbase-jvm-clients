/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.topology.ClusterIdentifier;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link RequestTracer} to provide new attributes.
 */
@Stability.Internal
public class RequestTracerDecorator implements RequestTracer {
  private final RequestTracer wrapped;
  private final Supplier<ClusterIdentifier> clusterIdentSupplier;

  public RequestTracerDecorator(RequestTracer wrapped, Supplier<ClusterIdentifier> clusterIdentSupplier) {
    this.wrapped = requireNonNull(wrapped);
    this.clusterIdentSupplier = clusterIdentSupplier;
  }

  @Override
  public RequestSpan requestSpan(String name, RequestSpan parent) {
    RequestSpan span = wrapped.requestSpan(name, parent);
    span.lowCardinalityAttribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    ClusterIdentifier clusterIdent = clusterIdentSupplier.get();
    if (clusterIdent != null) {
      span.attribute(TracingIdentifiers.ATTR_CLUSTER_NAME, clusterIdent.clusterName());
      span.attribute(TracingIdentifiers.ATTR_CLUSTER_UUID, clusterIdent.clusterUuid());
    }
    return span;
  }

  @Override
  public Mono<Void> start() {
    return wrapped.start();
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return wrapped.stop(timeout);
  }

  @Override
  public String toString() {
    return wrapped.getClass().getSimpleName();
  }
}
