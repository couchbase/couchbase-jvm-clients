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
import com.couchbase.client.core.endpoint.CircuitBreaker.State;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.error.InvalidArgumentException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * A circuit breaker that may be shared by many concurrent dispatch attempts.
 * <p>
 * Uses the same configuration and the same thresholds as {@code LazyCircuitBreaker},
 * which was designed for an endpoint with at most one request in flight. With many
 * attempts in flight, the lazy breaker has no way to tell which result belongs to which
 * dispatch, so an attempt dispatched long before the circuit opened can close it,
 * re-open it, or keep extending the sleep window.
 * <p>
 * This breaker ties each result to the dispatch attempt that produced it. An attempt
 * acquires a {@link Permit}, and reports its outcome through that permit. That allows
 * the breaker to:
 * <ul>
 *   <li>Ignore results from attempts dispatched before the circuit most recently opened.
 *       Each time the circuit opens, the breaker starts a new generation; permits from
 *       older generations are stale.
 *   <li>Let exactly one probe attempt through when the sleep window elapses, and let only
 *       the probe's outcome decide whether the circuit closes or re-opens. Until then,
 *       all other attempts are rejected, as with the lazy breaker.
 * </ul>
 * Callers must make sure every permit is eventually completed or released;
 * otherwise an unresolved probe holds the circuit half-open.
 * <p>
 * The rolling window is a tumbling window, as in the lazy breaker: counts reset once the
 * window has elapsed.
 * <p>
 * Always enabled. When the circuit breaker is disabled, use {@link AttemptCircuitBreaker#from}
 * to get a no-op breaker instead.
 * <p>
 * Thread-safe. State transitions happen under this object's monitor; the critical sections
 * are tiny compared to the cost of an HTTP request.
 * <p>
 * State transitions (closed, open, half-open) are logged at INFO level. Messages are built while
 * holding the lock, but logged after releasing it.
 */
@NullMarked
@Stability.Internal
public final class ConcurrentCircuitBreaker implements AttemptCircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(ConcurrentCircuitBreaker.class);

  /**
   * What the breaker protects, for log messages. For example, "query service on node1:8093".
   */
  private final String name;
  private final Consumer<String> transitionLog;

  private final int volumeThreshold;
  private final int errorThresholdPercentage;
  private final long sleepWindowNanos;
  private final long rollingWindowNanos;
  private final LongSupplier nanoClock;

  /**
   * Volatile so {@link #state()} can read it without locking. Only written while holding the lock.
   */
  private volatile State state;

  // Guarded by "this".
  private long generation;
  private long openedAt;
  private @Nullable Permit outstandingProbe;
  private long windowStart;
  private int windowTotal;
  private int windowFailures;

  /**
   * @param name what the breaker protects, for log messages. For example, "query service on node1:8093".
   */
  public ConcurrentCircuitBreaker(CircuitBreakerConfig config, String name) {
    this(config, name, System::nanoTime, log::info);
  }

  ConcurrentCircuitBreaker(CircuitBreakerConfig config, LongSupplier nanoClock) {
    this(config, "test", nanoClock, message -> {});
  }

  ConcurrentCircuitBreaker(
    CircuitBreakerConfig config,
    String name,
    LongSupplier nanoClock,
    Consumer<String> transitionLog
  ) {
    if (!config.enabled()) {
      throw InvalidArgumentException.fromMessage("This CircuitBreaker always needs to be enabled");
    }

    this.name = requireNonNull(name);
    this.transitionLog = requireNonNull(transitionLog);

    this.volumeThreshold = config.volumeThreshold();
    this.errorThresholdPercentage = config.errorThresholdPercentage();
    this.sleepWindowNanos = config.sleepWindow().toNanos();
    this.rollingWindowNanos = config.rollingWindow().toNanos();
    this.nanoClock = requireNonNull(nanoClock);

    this.state = State.CLOSED;
    this.windowStart = nanoClock.getAsLong();
  }

  @Override
  public State state() {
    return state;
  }

  @Override
  public @Nullable Permit tryAcquire() {
    Permit probe;
    String transition;

    synchronized (this) {
      switch (state) {
        case CLOSED:
          return new Permit(this, generation, false);

        case OPEN:
          if (nanoClock.getAsLong() - openedAt < sleepWindowNanos) {
            return null;
          }
          state = State.HALF_OPEN;
          probe = newProbe();
          transition = "Circuit breaker for " + name + " is now half-open: sending a probe request to see whether"
            + " the service has recovered. Other requests are rejected until the probe completes.";
          break;

        case HALF_OPEN:
          // Only reachable without an outstanding probe if the probe was released.
          return outstandingProbe == null ? newProbe() : null;

        default:
          throw new RuntimeException("Unexpected state: " + state);
      }
    }

    logTransition(transition);
    return probe;
  }

  private void logTransition(@Nullable String message) {
    if (message != null) {
      transitionLog.accept(message);
    }
  }

  // Must hold lock.
  private Permit newProbe() {
    Permit probe = new Permit(this, generation, true);
    outstandingProbe = probe;
    return probe;
  }

  private void onComplete(Permit permit, boolean success) {
    logTransition(completeAndDescribeTransition(permit, success));
  }

  /**
   * @return a description of the resulting state transition, or null if the state didn't change.
   */
  private synchronized @Nullable String completeAndDescribeTransition(Permit permit, boolean success) {
    if (permit.done) {
      return null;
    }
    permit.done = true;

    if (permit.generation != generation) {
      return null; // Stale: dispatched before the circuit last opened.
    }

    long now = nanoClock.getAsLong();

    if (permit.probe) {
      // A probe of the current generation is only ever issued (and outstanding) while half-open.
      if (success) {
        state = State.CLOSED;
        outstandingProbe = null;
        resetWindow(now);
        return "Circuit breaker for " + name + " is now closed: the probe request succeeded.";
      }

      open(now);
      return "Circuit breaker for " + name + " is now open again: the probe request failed."
        + " Rejecting requests for " + formatNanos(sleepWindowNanos) + " before trying again.";
    }

    if (state != State.CLOSED) {
      // Not reachable in practice: ordinary permits are only issued while closed,
      // and leaving the closed state always starts a new generation.
      return null;
    }

    if (now - windowStart > rollingWindowNanos) {
      resetWindow(now);
    }

    windowTotal++;
    if (!success) {
      windowFailures++;
    }

    if (windowTotal >= volumeThreshold
      && (long) windowFailures * 100 >= (long) errorThresholdPercentage * windowTotal) {
      // Describe the window before open() resets it.
      String transition = "Circuit breaker for " + name + " is now open: " + windowFailures + " of the last "
        + windowTotal + " requests failed, reaching the threshold of " + errorThresholdPercentage
        + "% (with at least " + volumeThreshold + " requests within " + formatNanos(rollingWindowNanos) + ")."
        + " Rejecting requests for " + formatNanos(sleepWindowNanos) + " before trying again.";
      open(now);
      return transition;
    }

    return null;
  }

  private synchronized void onRelease(Permit permit) {
    if (permit.done) {
      return;
    }
    permit.done = true;

    if (permit == outstandingProbe) {
      // The probe was abandoned without a result. Let the next attempt be the probe.
      outstandingProbe = null;
    }
  }

  // Must hold lock.
  private void open(long now) {
    generation++;
    state = State.OPEN;
    openedAt = now;
    outstandingProbe = null;
    resetWindow(now);
  }

  private static String formatNanos(long nanos) {
    long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
    return millis % 1000 == 0 ? (millis / 1000) + "s" : millis + "ms";
  }

  // Must hold lock.
  private void resetWindow(long now) {
    windowStart = now;
    windowTotal = 0;
    windowFailures = 0;
  }

  @Override
  public synchronized String toString() {
    return "ConcurrentCircuitBreaker{" +
      "name=" + name +
      ", state=" + state +
      ", generation=" + generation +
      ", windowTotal=" + windowTotal +
      ", windowFailures=" + windowFailures +
      '}';
  }

  @NullMarked
  public static final class Permit implements AttemptCircuitBreaker.Permit {
    private final ConcurrentCircuitBreaker breaker;
    private final long generation;
    private final boolean probe;

    // Guarded by breaker's lock.
    private boolean done;

    private Permit(ConcurrentCircuitBreaker breaker, long generation, boolean probe) {
      this.breaker = breaker;
      this.generation = generation;
      this.probe = probe;
    }

    @Override
    public void complete(boolean success) {
      breaker.onComplete(this, success);
    }

    @Override
    public void release() {
      breaker.onRelease(this);
    }

    /**
     * For tests, which check which permit is the probe.
     */
    boolean isProbe() {
      return probe;
    }

    @Override
    public String toString() {
      return "Permit{" +
        "generation=" + generation +
        ", probe=" + probe +
        '}';
    }
  }
}
