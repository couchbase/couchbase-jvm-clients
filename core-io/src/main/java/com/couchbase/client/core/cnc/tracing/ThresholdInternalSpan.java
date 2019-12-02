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

import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.RequestContext;

/**
 * Provides the basic span implementation for the {@link ThresholdRequestTracer}.
 * <p>
 * Most of these span methods are stubs since all the threshold tracer cares about is when the operation is complete
 * if it is over threshold.
 */
public class ThresholdInternalSpan implements InternalSpan {

  /**
   * Holds the associated tracer for this span.
   */
  private final ThresholdRequestTracer tracer;

  /**
   * Holds the associated request context once set.
   */
  private volatile RequestContext ctx;

  ThresholdInternalSpan(final ThresholdRequestTracer tracer, final String operationName, final RequestSpan parent) {
    this.tracer = tracer;
  }

  @Override
  public void finish() {
    tracer.finish(this);
  }

  @Override
  public void requestContext(final RequestContext ctx) {
    this.ctx = ctx;
  }

  public RequestContext requestContext() {
    return ctx;
  }

  @Override
  public void startDispatch() {
  }

  @Override
  public void stopDispatch() {
  }

  @Override
  public void startPayloadEncoding() {
  }

  @Override
  public void stopPayloadEncoding() {
  }

  @Override
  public RequestSpan toRequestSpan() {
    return ThresholdRequestSpan.INSTANCE;
  }

}
