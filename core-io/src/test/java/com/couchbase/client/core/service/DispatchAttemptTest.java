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

import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.CircuitBreaker.State;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DispatchAttemptTest {

  private static final Duration SLEEP_WINDOW = Duration.ofSeconds(5);

  private final AtomicLong clock = new AtomicLong(1_000_000);
  private final Semaphore slots = new Semaphore(1);
  private final List<Outcome> outcomes = new ArrayList<>();

  private static final class Outcome {
    final @Nullable Response response;
    final @Nullable Throwable error;

    Outcome(@Nullable Response response, @Nullable Throwable error) {
      this.response = response;
      this.error = error;
    }
  }

  /**
   * Records what it was asked, then applies the default callback.
   */
  private final CircuitBreaker.CompletionCallback recordingCallback = (response, error) -> {
    outcomes.add(new Outcome(response, error));
    return CircuitBreakerConfig.DEFAULT_COMPLETION_CALLBACK.apply(response, error);
  };

  /**
   * Trips after a single failure, so tests can tell whether an outcome counted as a failure.
   */
  private final ConcurrentCircuitBreaker breaker = new ConcurrentCircuitBreaker(
    CircuitBreakerConfig.builder()
      .volumeThreshold(1)
      .errorThresholdPercentage(100)
      .sleepWindow(SLEEP_WINDOW)
      .build(),
    clock::get
  );

  private final CompletableFuture<Response> requestFuture = new CompletableFuture<>();
  private final Request<Response> request = newRequest(requestFuture);

  @SuppressWarnings("unchecked")
  private static Request<Response> newRequest(CompletableFuture<Response> future) {
    Request<Response> request = mock(Request.class);
    when(request.response()).thenReturn(future);
    when(request.context()).thenReturn(mock(RequestContext.class));
    return request;
  }

  private DispatchAttempt newAttempt() {
    return newAttempt(recordingCallback);
  }

  private DispatchAttempt newAttempt(CircuitBreaker.@Nullable CompletionCallback callback) {
    ConcurrentCircuitBreaker.Permit permit = breaker.tryAcquire();
    assertNotNull(permit);
    assertTrue(slots.tryAcquire());
    return new DispatchAttempt(request, permit, callback, slots, new HostAndPort("node1", 8093));
  }

  private Outcome onlyOutcome() {
    assertEquals(1, outcomes.size(), "expected exactly one outcome");
    return outcomes.get(0);
  }

  @Test
  void responseCountsAsSuccess() {
    DispatchAttempt attempt = newAttempt();
    Response response = mock(Response.class);
    attempt.recordOutcome(response, null);
    attempt.finish();

    assertSame(response, onlyOutcome().response);
    assertEquals(State.CLOSED, breaker.state());
    assertEquals(1, slots.availablePermits());
  }

  @Test
  void errorResponsePassesErrorToCallback() {
    DispatchAttempt attempt = newAttempt();
    CouchbaseException error = new CouchbaseException("query failed");
    attempt.recordOutcome(null, error);

    assertSame(error, onlyOutcome().error);
    assertEquals(State.CLOSED, breaker.state()); // default callback: not a timeout, so a success
  }

  @Test
  void outcomeRecordedOnlyOnce() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordOutcome(mock(Response.class), null);
    attempt.recordFailure(new SocketTimeoutException("late"));
    attempt.recordOutcome(null, new CouchbaseException("late"));
    attempt.finish();

    onlyOutcome();
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void sdkTimeoutCountsAsFailure() {
    DispatchAttempt attempt = newAttempt();
    TimeoutException timeout = new UnambiguousTimeoutException("timed out", new CancellationErrorContext(request.context()));
    requestFuture.completeExceptionally(timeout);

    attempt.recordFailure(new IOException("Canceled"));

    assertSame(timeout, onlyOutcome().error);
    assertEquals(State.OPEN, breaker.state());
  }

  @Test
  void socketTimeoutCountsAsFailure() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordFailure(new SocketTimeoutException("connect timed out"));

    assertInstanceOf(TimeoutException.class, onlyOutcome().error);
    assertEquals(State.OPEN, breaker.state());
  }

  @Test
  void wrappedSocketTimeoutCountsAsFailure() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordFailure(new UncheckedIOException(new SocketTimeoutException("timeout")));

    assertInstanceOf(TimeoutException.class, onlyOutcome().error);
    assertEquals(State.OPEN, breaker.state());
  }

  @Test
  void otherIoFailurePassesCauseToCallback() {
    DispatchAttempt attempt = newAttempt();
    ConnectException refused = new ConnectException("Connection refused");
    attempt.recordFailure(refused);

    assertSame(refused, onlyOutcome().error);
    assertEquals(State.CLOSED, breaker.state()); // default callback: not a timeout
  }

  @Test
  void nonTimeoutCancellationIsNotCounted() {
    DispatchAttempt attempt = newAttempt();
    requestFuture.completeExceptionally(new RequestCanceledException(
      "cancelled", CancellationReason.SHUTDOWN, new CancellationErrorContext(request.context())
    ));

    attempt.recordFailure(new IOException("Canceled"));
    attempt.finish();

    assertTrue(outcomes.isEmpty());
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void finishWithoutOutcomeReleasesPermitAndSlot() {
    tripBreaker();
    clock.addAndGet(SLEEP_WINDOW.toNanos());

    DispatchAttempt probe = newAttempt();
    assertNull(breaker.tryAcquire());

    probe.finish();
    probe.finish(); // idempotent

    assertTrue(outcomes.isEmpty());
    assertEquals(1, slots.availablePermits());
    assertNotNull(breaker.tryAcquire(), "released probe should let the next attempt probe");
  }

  @Test
  void failedProbeReopensImmediately() {
    tripBreaker();
    clock.addAndGet(SLEEP_WINDOW.toNanos());

    DispatchAttempt probe = newAttempt();
    assertEquals(State.HALF_OPEN, breaker.state());
    probe.recordFailure(new SocketTimeoutException("connect timed out"));
    probe.finish();

    assertEquals(State.OPEN, breaker.state());
  }

  @Test
  void disabledBreakerSkipsCallback() {
    AttemptCircuitBreaker disabled = AttemptCircuitBreaker.from(CircuitBreakerConfig.enabled(false).build(), "test");
    AttemptCircuitBreaker.Permit permit = disabled.tryAcquire();
    assertNotNull(permit);
    assertTrue(slots.tryAcquire());

    DispatchAttempt attempt = new DispatchAttempt(request, permit, recordingCallback, slots, new HostAndPort("node1", 8093));
    attempt.recordOutcome(mock(Response.class), null);
    attempt.recordFailure(new SocketTimeoutException());
    attempt.finish();

    assertTrue(outcomes.isEmpty(), "callback should not be called when the circuit breaker is disabled");
    assertEquals(1, slots.availablePermits());
  }

  @Test
  void throwingCallbackCountsAsSuccess() {
    DispatchAttempt attempt = newAttempt((response, error) -> {
      throw new RuntimeException("oops");
    });
    attempt.recordFailure(new SocketTimeoutException());
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void connectTimeoutCountsAsFailureWithoutCallback() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordConnectFailure(new SocketTimeoutException("connect timed out"));
    attempt.finish();

    assertTrue(outcomes.isEmpty(), "connect failures should not be passed to the callback");
    assertEquals(State.OPEN, breaker.state());
    assertEquals(1, slots.availablePermits());
  }

  @Test
  void sdkTimeoutWhileConnectingIsNotCounted() {
    DispatchAttempt attempt = newAttempt();
    requestFuture.completeExceptionally(
      new UnambiguousTimeoutException("timed out", new CancellationErrorContext(request.context()))
    );
    attempt.recordConnectFailure(new IOException("Canceled"));
    attempt.finish();

    assertTrue(outcomes.isEmpty());
    assertEquals(State.CLOSED, breaker.state());
    assertEquals(1, slots.availablePermits());
  }

  @Test
  void sdkTimeoutRacingConnectTimeoutIsNotCounted() {
    DispatchAttempt attempt = newAttempt();
    requestFuture.completeExceptionally(
      new UnambiguousTimeoutException("timed out", new CancellationErrorContext(request.context()))
    );
    // The SDK timeout completed the request first; don't blame the node for the socket timeout.
    attempt.recordConnectFailure(new SocketTimeoutException("connect timed out"));

    assertTrue(outcomes.isEmpty());
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void sdkTimeoutWhileConnectingReleasesProbe() {
    tripBreaker();
    clock.addAndGet(SLEEP_WINDOW.toNanos());

    DispatchAttempt probe = newAttempt();
    requestFuture.completeExceptionally(
      new UnambiguousTimeoutException("timed out", new CancellationErrorContext(request.context()))
    );
    probe.recordConnectFailure(new IOException("Canceled"));
    probe.finish();

    assertEquals(State.HALF_OPEN, breaker.state());
    assertNotNull(breaker.tryAcquire(), "released probe should let the next attempt probe");
  }

  @Test
  void connectionRefusedIsNotCounted() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordConnectFailure(new ConnectException("Connection refused"));
    attempt.finish();

    assertTrue(outcomes.isEmpty());
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void tlsFailureIsNotCounted() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordConnectFailure(new SSLHandshakeException("PKIX path building failed"));

    assertTrue(outcomes.isEmpty());
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void cancellationWhileConnectingIsNotCounted() {
    DispatchAttempt attempt = newAttempt();
    requestFuture.completeExceptionally(new RequestCanceledException(
      "cancelled", CancellationReason.SHUTDOWN, new CancellationErrorContext(request.context())
    ));
    // Even if the cancellation surfaces as a timeout from the socket.
    attempt.recordConnectFailure(new SocketTimeoutException("timeout"));

    assertTrue(outcomes.isEmpty());
    assertEquals(State.CLOSED, breaker.state());
  }

  @Test
  void connectTimeoutOnProbeReopens() {
    tripBreaker();
    clock.addAndGet(SLEEP_WINDOW.toNanos());

    DispatchAttempt probe = newAttempt();
    assertEquals(State.HALF_OPEN, breaker.state());
    probe.recordConnectFailure(new SocketTimeoutException("connect timed out"));
    probe.finish();

    assertEquals(State.OPEN, breaker.state());
  }

  @Test
  void refusedProbeLetsNextAttemptProbe() {
    tripBreaker();
    clock.addAndGet(SLEEP_WINDOW.toNanos());

    DispatchAttempt probe = newAttempt();
    probe.recordConnectFailure(new ConnectException("Connection refused"));
    probe.finish();

    assertEquals(State.HALF_OPEN, breaker.state());
    assertNotNull(breaker.tryAcquire(), "released probe should let the next attempt probe");
  }

  @Test
  void connectTimeoutWithDisabledBreakerIsHarmless() {
    AttemptCircuitBreaker disabled = AttemptCircuitBreaker.from(CircuitBreakerConfig.enabled(false).build(), "test");
    AttemptCircuitBreaker.Permit permit = disabled.tryAcquire();
    assertNotNull(permit);
    assertTrue(slots.tryAcquire());

    DispatchAttempt attempt = new DispatchAttempt(request, permit, recordingCallback, slots, new HostAndPort("node1", 8093));
    attempt.recordConnectFailure(new SocketTimeoutException("connect timed out"));
    attempt.finish();

    assertTrue(outcomes.isEmpty());
    assertEquals(1, slots.availablePermits());
  }

  private void tripBreaker() {
    DispatchAttempt attempt = newAttempt();
    attempt.recordFailure(new SocketTimeoutException());
    attempt.finish();
    assertEquals(State.OPEN, breaker.state());
    outcomes.clear();
  }
}
