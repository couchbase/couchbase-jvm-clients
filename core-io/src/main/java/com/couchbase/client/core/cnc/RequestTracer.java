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
   * A common name for the dispatch span that implementations should use.
   */
  String DISPATCH_SPAN_NAME = "dispatch_to_server";

  /**
   * A common name for the value encode span that implementations should use.
   */
  String PAYLOAD_ENCODING_SPAN_NAME = "request_encoding";

  /**
   * The identifier commonly used to identify the kv service.
   */
  String SERVICE_IDENTIFIER_KV = "kv";

  /**
   * The identifier commonly used to identify the query service.
   */
  String SERVICE_IDENTIFIER_QUERY = "query";

  /**
   * The identifier commonly used to identify the search service.
   */
  String SERVICE_IDENTIFIER_SEARCH = "search";

  /**
   * The identifier commonly used to identify the view service.
   */
  String SERVICE_IDENTIFIER_VIEW = "view";

  /**
   * The identifier commonly used to identify the analytics service.
   */
  String SERVICE_IDENTIFIER_ANALYTICS = "analytics";

  /**
   * Creates a new span that represents a full request/response lifecycle in the SDK.
   *
   * @param operationName the name of the toplevel operation (i.e. "get")
   * @param parent the parent, can be null.
   * @return an internal span representing the toplevel request.
   */
  InternalSpan internalSpan(String operationName, RequestSpan parent);

  /**
   * Creates a new span that is created from the underlying tracer.
   *
   * @param operationName the name of the toplevel operation (i.e. "get")
   * @param parent a possible parent.
   * @return a request span that wraps the actual tracer implementation span.
   */
  RequestSpan requestSpan(String operationName, RequestSpan parent);

  /**
   * Starts the tracer if it hasn't been started, might be a noop depending on the implementation.
   */
  Mono<Void> start();

  /**
   * Stops the tracer if it has been started previously, might be a noop depending on the implementation.
   */
  Mono<Void> stop(Duration timeout);

}
