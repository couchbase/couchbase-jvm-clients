/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.cnc.metrics.ResponseMetricIdentifier;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.deps.io.grpc.Deadline;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.GenericErrorContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;

/**
 * Holds onto a GRPC request, along with pertaining mutable and immutable state bound to the request's lifetime.
 */
@Stability.Internal
public class ProtostellarRequest<TGrpcRequest> {
  private static final Logger log = LoggerFactory.getLogger(ProtostellarRequest.class);

  private final CoreProtostellar core;
  private final @Nullable RequestSpan span;
  private final long absoluteTimeout;

  /**
   * The time it took to encode the payload (if any).
   */
  private long encodeDurationNanos;
  private final RetryStrategy retryStrategy;

  private final long createdAt = System.nanoTime();
  protected final ServiceType serviceType;
  private final String requestName;

  /** Whether this request definitely will not effect any change on the server. */

  private final boolean readonly;
  private final Duration timeout;
  private final Deadline deadline;
  private final Map<String, Object> clientContext;


  private final TGrpcRequest request;
  private long logicallyCompletedAt;
  private long lastDispatchDurationNanos;
  private long totalDispatchDurationNanos;
  private int retryAttempts;
  private Set<RetryReason> retryReasons;
  private CancellationReason cancellationReason;
  /**
   * The state model is slightly different to BaseRequest's.  A request can either return success
   * or an exception to the user.  We don't regard cancellation as a separate state, it's simply
   * a form of raising an exception.  It provides a simpler model where the state may only be
   * changed in logicallyComplete.
   */
  private volatile State state = State.INCOMPLETE;

  /**
   * Allows more specialised requests with more context, to fill in extra information to the
   * ErrorContext.
   */
  private @Nullable Consumer<Map<String, Object>> contextSupplier;

  public ProtostellarRequest(TGrpcRequest request,
                             CoreProtostellar core,
                             ServiceType serviceType,
                             String requestName,
                             RequestSpan span,
                             Duration timeout,
                             boolean readonly,
                             RetryStrategy retryStrategy,
                             Map<String, Object> clientContext,
                             long encodeDurationNanos,
                             @Nullable Consumer<Map<String, Object>> contextSupplier) {
    this.request = request;
    this.core = core;
    this.serviceType = serviceType;
    this.requestName = requestName;
    this.span = span;
    this.absoluteTimeout = System.nanoTime() + timeout.toNanos();
    this.readonly = readonly;
    this.retryStrategy = retryStrategy;
    this.timeout = timeout;
    this.deadline = convertTimeout(timeout);
    this.clientContext = clientContext;
    this.encodeDurationNanos = encodeDurationNanos;
    this.contextSupplier = contextSupplier;
  }

  public TGrpcRequest request() {
    return request;
  }

  public RequestSpan span() {
    return span;
  }

  /**
   * Crucial to always ultimately call this on every request, and just once.
   */
  public void raisedResponseToUser(@Nullable Throwable err) {
    if (state != State.INCOMPLETE) {
      throw new IllegalStateException("Trying to raise a response multiple times on the same request - internal bug");
    }

    state = (err == null) ? State.SUCCEEDED : State.FAILED;

    if (span != null) {
      if (!CbTracing.isInternalSpan(span)) {
        TracingDecorator tip = core.context().coreResources().tracingDecorator();
        tip.provideAttr(TracingAttribute.RETRIES, span, retryAttempts());
        if (err != null) {
          span.recordException(err);
          span.status(RequestSpan.StatusCode.ERROR);
        }
      }
      span.end();
    }

    if (!(core.context().environment().meter() instanceof NoopMeter)) {
      long latency = logicalRequestLatency();
      boolean isDefaultLoggingMeter = core.context().coreResources().meter().isDefaultLoggingMeter();
      if (latency > 0) {
        ResponseMetricIdentifier rmi = new ResponseMetricIdentifier(serviceType.id(),
                requestName,
                // These are not currently supported for Protostellar
                null, null, null,
                null,
                // This is not available on Protostellar
                null,
                isDefaultLoggingMeter);
        try {
          core.responseMetric(rmi).recordValue(latency);
        } catch (Exception e) {
          log.warn("Failed to record request latency ({}). {}", Duration.ofNanos(latency), context(), e);
        }
      }
    }
  }

  public Duration timeout() {
    return timeout;
  }

  public Deadline deadline() {
    return deadline;
  }

  public long absoluteTimeout() {
    return absoluteTimeout;
  }

  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  public boolean timeoutElapsed() {
    return (this.absoluteTimeout - System.nanoTime()) <= 0;
  }

  public ProtostellarRequestBehaviour cancel(CancellationReason reason) {
    this.cancellationReason = reason;

    String msg = this.getClass().getSimpleName() + ", Reason: " + reason;
    CancellationErrorContext ctx = new CancellationErrorContext(context());
    RuntimeException exception = new RequestCanceledException(msg, reason, ctx);

    return ProtostellarRequestBehaviour.fail(exception);
  }

  public boolean readonly() {
    return readonly;
  }

  public long logicalRequestLatency() {
    if (logicallyCompletedAt == 0 || logicallyCompletedAt <= createdAt) {
      return 0;
    }
    return logicallyCompletedAt - createdAt;
  }
  //
  public void incrementRetryAttempts(Duration duration, RetryReason reason) {
    retryAttempts += 1;
    if (retryReasons == null) {
      retryReasons = new HashSet<>();
    }
    retryReasons.add(reason);
  }

  public GenericErrorContext context() {
    Map<String, Object> input = new HashMap<>();

    if (contextSupplier != null) {
      contextSupplier.accept(input);
    }

    input.put("readonly", readonly);
    input.put("requestName", requestName);
    input.put("retried", retryAttempts);
    input.put("completed", completed());
    input.put("timeoutMs", timeout.toMillis());
    if (cancellationReason != null) {
      input.put("cancelled", true);
      input.put("reason", cancellationReason);
    }
    if (clientContext != null) {
      input.put("clientContext", clientContext);
    }
    if (retryReasons != null) {
      input.put("retryReasons", retryReasons);
    }
    long logicalLatency = logicalRequestLatency();
    if (lastDispatchDurationNanos != 0 || logicalLatency != 0 || encodeDurationNanos != 0) {
      HashMap<String, Long> timings = new HashMap<>();
      if (lastDispatchDurationNanos != 0) {
        timings.put("lastDispatchMicros", TimeUnit.NANOSECONDS.toMicros(lastDispatchDurationNanos));
      }
      if (totalDispatchDurationNanos != 0) {
        timings.put("totalDispatchMicros", TimeUnit.NANOSECONDS.toMicros(totalDispatchDurationNanos));
      }
      if (logicalLatency != 0) {
        timings.put("totalMicros", TimeUnit.NANOSECONDS.toMicros(logicalLatency));
      }
      if (encodeDurationNanos != 0) {
        timings.put("encodingMicros", TimeUnit.NANOSECONDS.toMicros(encodeDurationNanos));
      }
      input.put("timings", timings);
    }

    return new GenericErrorContext(input, null);
  }

  public int retryAttempts() {
    return retryAttempts;
  }

  public void dispatchDuration(long durationNanos) {
    lastDispatchDurationNanos = durationNanos;
    totalDispatchDurationNanos += durationNanos;
  }

  public boolean completed() {
    return state != State.INCOMPLETE;
  }

  public long createdAt() {
    return createdAt;
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  public @Nullable CancellationReason cancellationReason() {
    return cancellationReason;
  }

  public boolean failed() {
    return state == State.FAILED;
  }

  public boolean succeeded() {
    return state == State.SUCCEEDED;
  }

  /**
   * Represents the states this request can be in.
   */
  private enum State {

    /**
     * This request is not complete yet.
     */
    INCOMPLETE,

    /**
     * This request has been completed successfully.
     */
    SUCCEEDED,

    /**
     * This request has been completed with failure.
     */
    FAILED
  }
}
