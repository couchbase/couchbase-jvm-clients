/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;

import java.util.Map;

/**
 * Additional context which might be attached to an individual {@link Request}.
 *
 * @since 2.0.0
 */
public class RequestContext extends CoreContext {

  /**
   * Holds the dispatch latency if set already (or at all).
   */
  private volatile long dispatchLatency;

  /**
   * The request ID associated.
   */
  private final long requestId;

  public RequestContext(CoreContext ctx, final long id) {
    super(ctx.id(), ctx.environment());
    this.requestId = id;
  }

  /**
   * Returns the duration of the dispatch phase if set.
   *
   * @return the duration of the dispatch phase.
   */
  @Stability.Volatile
  public long dispatchLatency() {
    return dispatchLatency;
  }

  /**
   * Allows to set the dispatch duration of the request.
   *
   * @param dispatchLatency the duration.
   */
  @Stability.Internal
  public void dispatchLatency(long dispatchLatency) {
    this.dispatchLatency = dispatchLatency;
  }


  @Override
  protected void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("requestId", requestId);
  }
}
