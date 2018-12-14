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

package com.couchbase.client.core.endpoint;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This {@link CircuitBreaker} tracks its states in a lazy fashion.
 *
 * <p>Lazy means that its state is evaluated at the time of a request, so there is no overhead
 * to be paid if no traffic is flowing through the associated endpoint.</p>
 *
 * <p>The way this circuit breaker works is the following: it starts out as CLOSED, so operations
 * can flow through freely. For every succeeding op we track it towards a total rolling count, and
 * every configured window we clear the counts for the next window. If a response fails, then it
 * is counted towards the failed requests as well and we check if the circuit is over threshold
 * and should be tripped. If we trip, then it goes into an OPEN state. At this point, requests
 * are not allowed to go through until the sleep window elapsed. The next request can go through
 * and sets it into HALF_OPEN. this request acts as a canary! If it completes the circuit closes
 * again. if it fails, then it goes back into OPEN and the whole sleep process starts again.</p>
 *
 * <p>In addition, the endpoint can always {@link #reset()} its state, which usually happens
 * when the channel is reset.</p>
 *
 * @since 2.0.0
 */
class LazyCircuitBreaker implements CircuitBreaker {

  /**
   * Current configuration.
   */
  private final CircuitBreakerConfig config;

  /**
   * Duration in nanoseconds of the rolling window.
   */
  private final long rollingWindow;

  /**
   * Time of the sleeping window in nanoseconds.
   */
  private final long sleepingWindow;

  /**
   * Current state of this breaker.
   */
  private final AtomicReference<State> state;

  /**
   * Holds the base marker for the current tracking window as an absolute
   * nano timestamp.
   */
  private volatile long windowStartTimestamp;

  /**
   * Counts all ops in the current window.
   */
  private final AtomicLong totalInWindow;

  /**
   * Counts failed ops in the current window.
   */
  private final AtomicLong failureInWindow;

  /**
   * Time in nanos when the circuit opened.
   */
  private volatile long circuitOpened;

  /**
   * Creates a new {@link LazyCircuitBreaker}.
   *
   * @param config the config for this circuit breaker.
   */
  LazyCircuitBreaker(final CircuitBreakerConfig config) {
    if (!config.enabled()) {
      throw new IllegalArgumentException("This CircuitBreaker always needs to be enabled");
    }

    this.config = config;
    this.state = new AtomicReference<>();
    this.rollingWindow = config.rollingWindow().toNanos();
    this.sleepingWindow = config.sleepWindow().toNanos();
    this.totalInWindow = new AtomicLong();
    this.failureInWindow = new AtomicLong();
    reset();
  }

  @Override
  public void track() {
    state.compareAndSet(State.OPEN, State.HALF_OPEN);
  }

  @Override
  public void reset() {
    state.set(State.CLOSED);
    circuitOpened = -1;
    totalInWindow.set(0);
    failureInWindow.set(0);
    windowStartTimestamp = -1;
  }

  @Override
  public boolean allowsRequest() {
    State state = state();
    boolean sleepingWindowElapsed = System.nanoTime() > (circuitOpened + sleepingWindow);
    return state == State.CLOSED || (state == State.OPEN && sleepingWindowElapsed);
  }

  @Override
  public State state() {
    return state.get();
  }

  /**
   * Cleans up the current rolling window in case we rolled over.
   */
  private void cleanRollingWindow() {
    long now = System.nanoTime();
    if (now > (windowStartTimestamp + rollingWindow)) {
      windowStartTimestamp = now;
      totalInWindow.set(0);
      failureInWindow.set(0);
    }
  }

  /**
   * Checks if we have tripped and if so performs side effects to set the circuit
   * breaker into the right state.
   */
  private void checkIfTripped() {
    if (totalInWindow.get() < config.volumeThreshold()) {
      return;
    }

    int percentThreshold = config.errorThresholdPercentage();
    long currentThreshold = (long) ((failureInWindow.get() * 1.0f / totalInWindow.get()) * 100);
    if (currentThreshold >= percentThreshold) {
      state.set(State.OPEN);
      circuitOpened = System.nanoTime();
    }
  }

  /**
   * Mark a tracked request as failed.
   */
  @Override
  public void markFailure() {
    long now = System.nanoTime();
    if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
      circuitOpened = now;
    } else {
      cleanRollingWindow();
      totalInWindow.incrementAndGet();
      failureInWindow.incrementAndGet();
      checkIfTripped();
    }
  }

  /**
   * Mark a tracked request as success.
   */
  @Override
  public void markSuccess() {
    if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
      reset();
    } else {
      cleanRollingWindow();
      totalInWindow.incrementAndGet();
    }
  }

}
