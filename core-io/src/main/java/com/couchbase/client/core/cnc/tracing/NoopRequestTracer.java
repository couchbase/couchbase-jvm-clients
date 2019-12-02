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

package com.couchbase.client.core.cnc.tracing;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * A simple NOOP implementation of the tracer, useful if tracing needs to be disabled completely.
 */
public class NoopRequestTracer implements RequestTracer {

  @Override
  public InternalSpan internalSpan(final String operationName, final RequestSpan parent) {
    return NoopInternalSpan.INSTANCE;
  }

  @Override
  public Mono<Void> start() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> stop(final Duration timeout) {
    return Mono.empty();
  }

  @Override
  public RequestSpan requestSpan(final String operationName, final RequestSpan parent) {
    return NoopRequestSpan.INSTANCE;
  }

}
