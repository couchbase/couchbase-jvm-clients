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

package com.couchbase.client.core.error.context;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.diagnostics.WaitUntilReadyContext;
import com.couchbase.client.core.kv.RangeScanContext;
import com.couchbase.client.core.msg.RequestContext;

import java.util.Map;

/**
 * When a cancellation (i.e. timeout) occurs we do only have the information available that is currently with the
 * request, so this context is not service-specific and just dumps what's in there.
 */
@Stability.Uncommitted
public class CancellationErrorContext extends ErrorContext {

  private final RequestContext requestContext;
  private final AbstractContext context;

  public CancellationErrorContext(RequestContext requestContext) {
    super(null);
    this.requestContext = requestContext;
    this.context = null;
  }

  public CancellationErrorContext(AbstractContext context) {
    super(null);
    this.context = context;
    this.requestContext = null;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (requestContext != null) {
      requestContext.injectExportableParams(input);
    }
    if (context != null) {
      context.injectExportableParams(input);
    }
  }

  /**
   * Returns the underlying request context (if present) for debug reasons.
   */
  public RequestContext requestContext() {
    return requestContext;
  }

  /**
   * Returns information specific to a cancelled "wait until ready" request.
   *
   * @return (nullable) Returns null if no information is available,
   * or if this error context is not associated with a "wait until ready" request.
   */
  public WaitUntilReadyContext getWaitUntilReadyContext() {
    return context instanceof WaitUntilReadyContext ? (WaitUntilReadyContext) context : null;
  }

  /**
   * Returns information specific to a cancelled "range scan" request.
   *
   * @return (nullable) Returns null if no information is available,
   * or if this error context is not associated with a "range scan" request.
   */
  public RangeScanContext getRangeScanContext() {
    return context instanceof RangeScanContext ? (RangeScanContext) context : null;
  }

}
