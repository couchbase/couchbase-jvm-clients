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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Base class for all {@link Request Requests}.
 *
 * @since 2.0.0
 */
public class BaseRequest<RES extends Response> implements Request<RES> {

  /**
   * Atomic updater for the {@link #state} field.
   */
  private static final AtomicReferenceFieldUpdater<BaseRequest, State> STATE_UPDATER =
    AtomicReferenceFieldUpdater.newUpdater(BaseRequest.class, State.class, "state");

  private final Duration timeout;
  private final RequestContext ctx;
  private final CompletableFuture<RES> response;

  /**
   * The {@link State} this {@link Request} is in at the moment.
   *
   * <p>Do not rename this field without updating the {@link #STATE_UPDATER}!</p>
   */
  private volatile State state = State.INCOMPLETE;

  /**
   * Creates a basic request that has all the required properties to be
   * executed in general.
   *
   * @param timeout the timeout of the request.
   * @param ctx the context if provided.
   */
  public BaseRequest(final Duration timeout, final RequestContext ctx) {
    if (timeout == null) {
      throw new IllegalArgumentException("A Timeout must be provided");
    }
    this.timeout = timeout;
    this.ctx = ctx;
    this.response = new CompletableFuture<>();
  }

  @Override
  public CompletableFuture<RES> response() {
    return response;
  }

  @Override
  public void succeed(RES result) {
    if (STATE_UPDATER.compareAndSet(this, State.INCOMPLETE, State.SUCCESS)) {
      response.complete(result);
    }
  }

  @Override
  public void fail(Throwable error) {
    if (STATE_UPDATER.compareAndSet(this, State.INCOMPLETE, State.FAILURE)) {
      response.completeExceptionally(error);
    }
  }

  @Override
  public RequestContext context() {
    return ctx;
  }

  @Override
  public Duration timeout() {
    return timeout;
  }

  /**
   * Represents the states this {@link Request} can be in.
   *
   * <p>Right now it is only used to internally track different modes in one volatile
   * variable instead of many.</p>
   */
  private enum State {

    /**
     * This {@link Request} is not complete yet.
     */
    INCOMPLETE,

    /**
     * This request has been completed successfully.
     */
    SUCCESS,

    /**
     * This request has been completed with failure.
     */
    FAILURE

  }
}
