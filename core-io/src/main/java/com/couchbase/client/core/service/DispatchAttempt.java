/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static java.util.Objects.requireNonNull;

/**
 * One attempt to dispatch a request to a node: a single OkHttp call.
 * <p>
 * Owns two resources, and makes sure each is given back exactly once:
 * <ul>
 *   <li>A circuit breaker permit. The attempt's outcome is recorded as soon as it is known
 *       (typically when the first result row or an error response arrives), using the
 *       configured {@link CircuitBreaker.CompletionCallback} to decide whether the outcome
 *       counts as a success or a failure.
 *   <li>A slot in the node's in-flight limit. Held until the HTTP exchange is completely
 *       finished, including streaming the response body, because until then the attempt
 *       occupies a connection to the node.
 * </ul>
 * Not every failure reflects on the node's health. If the request was cancelled for a
 * reason other than a timeout (for example, the user cancelled it, or the SDK is shutting
 * down), the permit is released without recording an outcome.
 */
@NullMarked
@Stability.Internal
public final class DispatchAttempt {
  private static final Logger log = LoggerFactory.getLogger(DispatchAttempt.class);

  private final Request<?> request;
  private final AttemptCircuitBreaker.Permit permit;
  private final CircuitBreaker.CompletionCallback completionCallback;
  private final Semaphore inFlightSlots;
  private final HostAndPort remote;

  private final AtomicBoolean outcomeResolved = new AtomicBoolean();
  private final AtomicBoolean finished = new AtomicBoolean();

  /**
   * @param completionCallback decides whether an outcome counts as success or failure.
   * Not called if the permit does not track outcomes (because the circuit breaker is disabled).
   * @param inFlightSlots the attempt has already acquired one permit from this semaphore,
   * and releases it when the attempt finishes.
   */
  DispatchAttempt(
    Request<?> request,
    AttemptCircuitBreaker.Permit permit,
    CircuitBreaker.CompletionCallback completionCallback,
    Semaphore inFlightSlots,
    HostAndPort remote
  ) {
    this.request = requireNonNull(request);
    this.permit = requireNonNull(permit);
    this.completionCallback = requireNonNull(completionCallback);
    this.inFlightSlots = requireNonNull(inFlightSlots);
    this.remote = requireNonNull(remote);
  }

  /**
   * Records the outcome of an attempt where the node responded.
   * <p>
   * Pass the response or error as the SDK sees it, as if the request were completing
   * with that result. Has no effect if the outcome was already resolved.
   */
  void recordOutcome(@Nullable Response response, @Nullable Throwable error) {
    if (!outcomeResolved.compareAndSet(false, true)) {
      return;
    }

    if (!permit.tracksOutcome()) {
      // Same as the Netty implementation, which doesn't call the callback when the circuit breaker is disabled.
      permit.release();
      return;
    }

    boolean success;
    try {
      success = completionCallback.apply(response, error);
    } catch (Throwable t) {
      log.warn("Circuit breaker completion callback threw an exception; treating outcome as success.", t);
      success = true;
    }
    permit.complete(success);
  }

  /**
   * Records the outcome of an attempt that ended without a usable response:
   * the call failed, or reading the response body failed.
   * Has no effect if the outcome was already resolved.
   */
  void recordFailure(Throwable cause) {
    Throwable requestError = errorIfCompletedExceptionally(request.response());

    if (requestError instanceof TimeoutException) {
      // The SDK's timeout fired while this attempt was in flight, and cancelled the call.
      recordOutcome(null, requestError);
      return;
    }

    if (requestError instanceof RequestCanceledException) {
      // Cancelled for a reason that says nothing about the node's health.
      releaseOutcome();
      return;
    }

    if (hasCause(cause, SocketTimeoutException.class)) {
      // OkHttp's own read timeout. Report it like the SDK timeout,
      // so callbacks that only count timeouts (like the default) see it.
      recordOutcome(null, new AmbiguousTimeoutException(
        "Timed out waiting for " + remote,
        new CancellationErrorContext(request.context())
      ));
      return;
    }

    recordOutcome(null, cause);
  }

  /**
   * Records the outcome of an attempt that failed before the request was sent to the node:
   * while resolving the node's address, connecting, or doing the TLS handshake.
   * <p>
   * Does not consult the completion callback. As with the Netty implementation, the callback
   * only judges requests that reached the node. Instead:
   * <ul>
   *   <li>OkHttp's connect timeout counts as a failure. The node did not accept the
   *       connection in time, and each such attempt ties up an in-flight slot while it waits,
   *       so this is exactly the kind of unresponsive node the circuit breaker exists to
   *       route around.
   *   <li>Anything else (connection refused, unknown host, TLS errors, cancellation)
   *       is not counted. Those fail fast, or reflect configuration problems
   *       instead of the node's health.
   *   <li>That includes the SDK's request timeout firing while the attempt was connecting.
   *       The request may have spent most of its time elsewhere (retrying, or waiting
   *       to be retried), and reached this node with little time left, so the timeout
   *       says little about this node's health.
   * </ul>
   * Has no effect if the outcome was already resolved.
   */
  void recordConnectFailure(Throwable cause) {
    // Only trust a socket timeout if the request was still in progress.
    // If the request already completed (SDK timeout, cancellation, etc.), the call
    // was cancelled as a result, and the exception reflects that instead of the node's health.
    boolean connectTimedOut = !request.response().isDone()
      && hasCause(cause, SocketTimeoutException.class);

    if (!connectTimedOut) {
      releaseOutcome();
      return;
    }

    if (outcomeResolved.compareAndSet(false, true)) {
      permit.complete(false); // ignored if the permit doesn't track outcomes
    }
  }

  /**
   * Gives back the circuit breaker permit, without recording an outcome
   * unless one was already recorded, and frees the in-flight slot.
   * <p>
   * Call when the HTTP exchange is completely finished. Safe to call more than once.
   */
  void finish() {
    if (finished.compareAndSet(false, true)) {
      releaseOutcome();
      inFlightSlots.release();
    }
  }

  private void releaseOutcome() {
    if (outcomeResolved.compareAndSet(false, true)) {
      permit.release();
    }
  }

  private static @Nullable Throwable errorIfCompletedExceptionally(CompletableFuture<?> future) {
    if (!future.isCompletedExceptionally()) {
      return null;
    }
    // For the future itself (as opposed to a dependent stage), the throwable is not wrapped.
    return future.handle((result, error) -> error).getNow(null);
  }

  @Override
  public String toString() {
    return "DispatchAttempt{" +
      "remote=" + remote +
      ", permit=" + permit +
      ", outcomeResolved=" + outcomeResolved +
      ", finished=" + finished +
      '}';
  }
}
