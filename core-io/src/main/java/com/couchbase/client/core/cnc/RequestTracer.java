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

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.annotation.Stability;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * The {@link RequestTracer} describes the tracing abstraction in the SDK.
 * <p>
 * Various implementations exist, both as part of the core library but also as external modules that can be
 * attached (i.e. for OpenTracing and OpenTelemetry). It is recommended to use those modules and not write
 * your own tracer unless absolutely needed.
 */
@Stability.Volatile
public interface RequestTracer {

  /**
   * Creates a new request span with or without a parent.
   *
   * @param name the name of the toplevel operation (i.e. "cb.get")
   * @param parent a parent, if no parent is used supply null.
   * @return a request span that wraps the actual tracer implementation span.
   */
  RequestSpan requestSpan(String name, RequestSpan parent);

  /**
   * Starts the tracer if it hasn't been started, might be a noop depending on the implementation.
   */
  Mono<Void> start();

  /**
   * Stops the tracer if it has been started previously, might be a noop depending on the implementation.
   */
  Mono<Void> stop(Duration timeout);

}
