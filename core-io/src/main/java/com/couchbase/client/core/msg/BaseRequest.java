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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Base class for all {@link Request Requests}.
 *
 * @since 2.0.0
 */
public abstract class BaseRequest<R extends Response> implements Request<R> {

  /**
   * Generator for each request ID.
   */
  private static final AtomicLong REQUEST_ID = new AtomicLong();

  /**
   * Atomic updater for the {@link #state} field.
   */
  private static final AtomicReferenceFieldUpdater<BaseRequest, State> STATE_UPDATER =
    AtomicReferenceFieldUpdater.newUpdater(BaseRequest.class, State.class, "state");

  /**
   * Holds the unique ID for this request.
   */
  private final long id;

  /**
   * Holds the timeout of this request.
   */
  private final Duration timeout;

  /**
   * Holds the absolute timeout deadline.
   */
  private final long absoluteTimeout;

  /**
   * Holds the request context, if set.
   */
  private final RequestContext ctx;

  /**
   * Holds the internal future used to complete the response.
   */
  private final CompletableFuture<R> response;

  /**
   * Holds the current retry strategy in use.
   */
  private final RetryStrategy retryStrategy;

  /**
   * If present, holds the tracing request span.
   */
  private final RequestSpan requestSpan;

  /**
   * Stores the time when the request got created.
   */
  private final long createdAt;

  /**
   * The {@link State} this {@link Request} is in at the moment.
   *
   * <p>Do not rename this field without updating the {@link #STATE_UPDATER}!</p>
   */
  private volatile State state = State.INCOMPLETE;

  /**
   * If cancelled, contains the reason why it got cancelled.
   */
  private volatile CancellationReason cancellationReason;

  public BaseRequest(final Duration timeout, final CoreContext ctx,
                     final RetryStrategy retryStrategy) {
    this(timeout, ctx, retryStrategy, null);
  }

  /**
   * Creates a basic request that has all the required properties to be
   * executed in general.
   *
   * @param timeout the timeout of the request.
   * @param ctx the context if provided.
   */
  public BaseRequest(final Duration timeout, final CoreContext ctx,
                     final RetryStrategy retryStrategy, final RequestSpan requestSpan) {
    if (timeout == null) {
      throw InvalidArgumentException.fromMessage("A Timeout must be provided");
    }
    if (ctx == null) {
      throw InvalidArgumentException.fromMessage("A CoreContext must be provided");
    }
    this.timeout = timeout;
    this.createdAt = System.nanoTime();
    this.absoluteTimeout = createdAt + timeout.toNanos();
    this.response = new CompletableFuture<>();
    this.id = REQUEST_ID.incrementAndGet();
    this.ctx = new RequestContext(ctx, this);
    this.retryStrategy = retryStrategy == null ? ctx.environment().retryStrategy() : retryStrategy;

    if (requestSpan != null) {
      requestSpan.requestContext(this.ctx);
      requestSpan.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    }

    this.requestSpan = requestSpan;
  }

  @Override
  public CompletableFuture<R> response() {
    return response;
  }

  @Override
  public void succeed(R result) {
    if (STATE_UPDATER.compareAndSet(this, State.INCOMPLETE, State.SUCCEEDED)) {
      response.complete(result);
    }
  }

  @Override
  public void fail(Throwable error) {
    if (STATE_UPDATER.compareAndSet(this, State.INCOMPLETE, State.FAILED)) {
      response.completeExceptionally(error);
    }
  }

  @Override
  public void cancel(final CancellationReason reason) {
    if (STATE_UPDATER.compareAndSet(this, State.INCOMPLETE, State.CANCELLED)) {
      cancellationReason = reason;
      final Exception exception;

      final String msg = this.getClass().getSimpleName() + ", Reason: " + reason;
      final CancellationErrorContext ctx = new CancellationErrorContext(context());
      if (reason == CancellationReason.TIMEOUT) {
        exception = idempotent() ? new UnambiguousTimeoutException(msg, ctx) : new AmbiguousTimeoutException(msg, ctx);
      } else {
        exception = new RequestCanceledException(msg, reason, ctx);
      }

      response.completeExceptionally(exception);
    }
  }

  @Override
  public boolean completed() {
    return state != State.INCOMPLETE;
  }

  @Override
  public boolean succeeded() {
    return state == State.SUCCEEDED;
  }

  @Override
  public boolean failed() {
    return state == State.FAILED;
  }

  @Override
  public boolean cancelled() {
    return state == State.CANCELLED;
  }

  @Override
  public CancellationReason cancellationReason() {
    return cancellationReason;
  }

  @Override
  public RequestContext context() {
    return ctx;
  }

  @Override
  public Duration timeout() {
    return timeout;
  }

  @Override
  public boolean timeoutElapsed() {
    return (this.absoluteTimeout - System.nanoTime()) <= 0;
  }

  @Override
  public long absoluteTimeout() {
    return absoluteTimeout;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  @Override
  public Map<String, Object> serviceContext() {
    return null;
  }

  @Override
  public long createdAt() {
    return createdAt;
  }

  @Override
  public RequestSpan requestSpan() {
    return requestSpan;
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
    SUCCEEDED,

    /**
     * This request has been completed with failure.
     */
    FAILED,

    /**
     * This request has been cancelled before it could be completed.
     */
    CANCELLED
  }

}
