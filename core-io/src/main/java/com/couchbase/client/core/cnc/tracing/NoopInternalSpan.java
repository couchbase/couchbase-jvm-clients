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
import com.couchbase.client.core.msg.RequestContext;

/**
 * A simple NOOP implementation of the span, useful if tracing needs to be disabled completely.
 */
public class NoopInternalSpan implements InternalSpan {

  public static NoopInternalSpan INSTANCE = new NoopInternalSpan();

  private NoopInternalSpan() {
  }

  @Override
  public void finish() {
  }

  @Override
  public void requestContext(final RequestContext ctx) {
  }

  @Override
  public RequestContext requestContext() {
    return null;
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
}
