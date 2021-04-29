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
import com.couchbase.client.core.msg.RequestContext;

import java.time.Instant;

public class ThresholdRequestSpan implements RequestSpan {

  private volatile RequestContext requestContext;
  private final ThresholdLoggingTracer tracer;

  ThresholdRequestSpan(final ThresholdLoggingTracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public void attribute(String key, String value) { }

  @Override
  public void attribute(String key, boolean value) { }

  @Override
  public void attribute(String key, long value) { }

  @Override
  public void event(String name, Instant timestamp) { }

  @Override
  public void status(StatusCode status) { }

  @Override
  public void requestContext(RequestContext requestContext) {
    this.requestContext = requestContext;
  }

  RequestContext requestContext() {
    return requestContext;
  }

  @Override
  public void end() {
    tracer.finish(this);
  }

}
