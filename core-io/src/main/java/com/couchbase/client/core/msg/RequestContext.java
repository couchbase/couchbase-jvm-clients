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
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.HostAndPort;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.Validators.notNull;

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
   * The time when the request got logically completed.
   */
  private volatile long logicallyCompletedAt;

  /**
   * The time it took to encode the payload (if any).
   */
  private volatile long encodeLatency;

  /**
   * The request ID associated.
   */
  private final Request<? extends Response> request;

  /**
   * User-attached domain specific diagnostic information.
   */
  private volatile Map<String, Object> clientContext;

  /**
   * The hostname/ip where this request got last dispatched to.
   */
  private volatile HostAndPort lastDispatchedTo;

  /**
   * The hostname/ip where this request got last dispatched from.
   */
  private volatile HostAndPort lastDispatchedFrom;

  /**
   * Holds a set of retry reasons.
   */
  private volatile AtomicReference<Set<RetryReason>> retryReasons = new AtomicReference<>(null);

  /**
   * The number of times the attached request has been retried.
   */
  private final AtomicInteger retryAttempts;

  /**
   * The last retry duration for this request.
   */
  private volatile Duration lastRetryDuration;

  /**
   * Creates a new {@link RequestContext}.
   *
   * @param ctx the core context.
   * @param request the linked request.
   */
  @Stability.Internal
  public RequestContext(CoreContext ctx, final Request<? extends Response> request) {
    super(ctx.core(), ctx.id(), ctx.environment(), ctx.authenticator());
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

  @Stability.Volatile
  public long encodeLatency() {
    return encodeLatency;
  }

  @Stability.Internal
  public RequestContext encodeLatency(long encodeLatency) {
    this.encodeLatency = encodeLatency;
    return this;
  }

  /**
   * Signals that this request is completed fully, including streaming sections or logical sub-requests also being
   * completed (i.e. observe polling).
   */
  @Stability.Internal
  public RequestContext logicallyComplete() {
    this.logicallyCompletedAt = System.nanoTime();
    if (request.internalSpan() != null) {
      request.internalSpan().finish();
    }
    return this;
  }

  public int retryAttempts() {
    return retryAttempts.get();
  }

  public Set<RetryReason> retryReasons() {
    return retryReasons.get();
  }

  public Duration lastRetryDuration() {
    return lastRetryDuration;
  }

  /**
   * Returns the absolute nano time when the request got logically completed.
   */
  public long logicallyCompletedAt() {
    return logicallyCompletedAt;
  }

  /**
   * Returns the request latency once logically completed (includes potential "inner" operations like observe
   * calls).
   */
  public long logicalRequestLatency() {
    return logicallyCompletedAt - request.createdAt();
  }

  @Stability.Internal
  public RequestContext incrementRetryAttempts(final Duration lastRetryDuration, final RetryReason reason) {
    notNull(lastRetryDuration, "Retry Duration");
    notNull(reason, "Retry Reason");

    retryReasons.getAndUpdate(retryReasons -> {
      if (retryReasons == null) {
        retryReasons = EnumSet.of(reason);
      } else {
        retryReasons.add(reason);
      }
      return retryReasons;
    });
    retryAttempts.incrementAndGet();
    this.lastRetryDuration = lastRetryDuration;
    return this;
  }

  public HostAndPort lastDispatchedTo() {
    return lastDispatchedTo;
  }

  @Stability.Internal
  public RequestContext lastDispatchedTo(final HostAndPort lastDispatchedTo) {
    this.lastDispatchedTo = lastDispatchedTo;
    return this;
  }

  public HostAndPort lastDispatchedFrom() {
    return lastDispatchedFrom;
  }

  @Stability.Internal
  public RequestContext lastDispatchedFrom(final HostAndPort lastDispatchedFrom) {
    this.lastDispatchedFrom = lastDispatchedFrom;
    return this;
  }

  /**
   * Returns the custom user payload of this request.
   *
   * @return the payload if set.
   */
  public Map<String, Object> clientContext() {
    return clientContext;
  }

  /**
   * Allows to set a custom payload for this request.
   *
   * @param clientContext the payload to set.
   */
  @Stability.Internal
  public RequestContext clientContext(final Map<String, Object> clientContext) {
    if (clientContext != null) {
      this.clientContext = clientContext;
    }
    return this;
  }

  public Request<? extends Response> request() {
    return request;
  }

  @Override
  protected void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("requestId", request.id());
    input.put("retried", retryAttempts());
    input.put("completed", request.completed());
    input.put("timeoutMs", request.timeout().toMillis());
    if (request.cancelled()) {
      input.put("cancelled", true);
      input.put("reason", request.cancellationReason());
    }
    if (clientContext != null) {
      input.put("clientContext", clientContext);
    }
    Map<String, Object> serviceContext = request.serviceContext();
    if (serviceContext != null) {
      input.put("service", serviceContext);
    }
    Set<RetryReason> retryReasons = retryReasons();
    if (retryReasons != null) {
      input.put("retryReasons", retryReasons);
    }
    long logicalLatency = logicalRequestLatency();
    if (dispatchLatency != 0 || logicalLatency != 0 || encodeLatency != 0) {
      HashMap<String, Long> timings = new HashMap<>();
      if (dispatchLatency != 0) {
        timings.put("dispatchMicros", TimeUnit.NANOSECONDS.toMicros(dispatchLatency));
      }
      if (logicalLatency != 0) {
        timings.put("totalMicros", TimeUnit.NANOSECONDS.toMicros(logicalLatency));
      }
      if (encodeLatency != 0) {
        timings.put("encodingMicros", TimeUnit.NANOSECONDS.toMicros(encodeLatency));
      }
      input.put("timings", timings);
    }
    if (lastDispatchedTo != null) {
      input.put("lastDispatchedTo", redactSystem(lastDispatchedTo));
    }
    if (lastDispatchedFrom != null) {
      input.put("lastDispatchedFrom", redactSystem(lastDispatchedFrom));
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
