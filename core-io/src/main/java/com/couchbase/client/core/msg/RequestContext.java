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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final Request<? extends Response> request;

  /**
   * Allows to attach a custom payload to the request for the user.
   */
  private volatile Map<String, Object> payload;

  /**
   * The number of times the attached request has been retried.
   */
  private final AtomicInteger retryAttempts;

  /**
   * Creates a new {@link RequestContext}.
   *
   * @param ctx the core context.
   * @param request the linked request.
   */
  @Stability.Internal
  public RequestContext(CoreContext ctx, final Request<? extends Response> request) {
    super(ctx.core(), ctx.id(), ctx.environment());
    this.request = request;
    this.retryAttempts = new AtomicInteger(0);
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
  public RequestContext dispatchLatency(long dispatchLatency) {
    this.dispatchLatency = dispatchLatency;
    return this;
  }

  public int retryAttempts() {
    return retryAttempts.get();
  }

  public RequestContext incrementRetryAttempt() {
    retryAttempts.incrementAndGet();
    return this;
  }

  /**
   * Returns the custom user payload of this request.
   *
   * @return the payload if set.
   */
  public Map<String, Object> payload() {
    return payload;
  }

  /**
   * Allows to set a custom payload for this request.
   *
   * @param payload the payload to set.
   */
  public RequestContext payload(final Map<String, Object> payload) {
    this.payload = payload;
    return this;
  }

  @Override
  protected void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("requestId", request.id());
    input.put("retried", retryAttempts());
    input.put("completed", request.completed());
    if (request.cancelled()) {
      input.put("cancelled", true);
      input.put("reason", request.cancellationReason());
    }
    if (payload != null) {
      input.put("payload", payload);
    }
    if (dispatchLatency != 0) {
      input.put("timings", new HashMap<>().put("dispatch", dispatchLatency));
    }
  }

  /**
   * Allows to cancel the attached {@link Request} from anywhere in the code.
   *
   * <p>If the operation is already completed (either successfully or failed) this
   * is an operation without side-effect.</p>
   */
  @Stability.Uncommitted
  public RequestContext cancel() {
    request.cancel(CancellationReason.CANCELLED_VIA_CONTEXT);
    return this;
  }

}
