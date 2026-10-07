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

import com.couchbase.client.core.endpoint.CircuitBreaker.State;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.service.ConcurrentCircuitBreaker.Permit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentCircuitBreakerTest {

  private static final Duration SLEEP_WINDOW = Duration.ofSeconds(5);
  private static final Duration ROLLING_WINDOW = Duration.ofMinutes(1);
  private static final int VOLUME_THRESHOLD = 4;

  private static final String NAME = "query service on node1:8093";

  private final AtomicLong clock = new AtomicLong(1_000_000);

  // Logged state transitions.
  private final List<String> transitions = new ArrayList<>();
  private volatile @Nullable ConcurrentCircuitBreaker current;
  private volatile boolean loggedWhileHoldingLock;

  private ConcurrentCircuitBreaker newBreaker() {
    return newBreaker(CircuitBreakerConfig.builder()
      .volumeThreshold(VOLUME_THRESHOLD)
      .errorThresholdPercentage(50)
      .sleepWindow(SLEEP_WINDOW)
      .rollingWindow(ROLLING_WINDOW)
      .build());
  }

  private ConcurrentCircuitBreaker newBreaker(CircuitBreakerConfig config) {
    ConcurrentCircuitBreaker cb = new ConcurrentCircuitBreaker(config, NAME, clock::get, message -> {
      ConcurrentCircuitBreaker c = current;
      if (c != null && Thread.holdsLock(c)) {
        loggedWhileHoldingLock = true;
      }
      synchronized (transitions) {
        transitions.add(message);
      }
    });
    current = cb;
    return cb;
  }

  private void advance(Duration d) {
    clock.addAndGet(d.toNanos());
  }

  private static Permit acquire(ConcurrentCircuitBreaker cb) {
    Permit p = cb.tryAcquire();
    assertNotNull(p, "expected a permit; breaker state = " + cb.state());
    return p;
  }

  private static void fail(ConcurrentCircuitBreaker cb, int times) {
    for (int i = 0; i < times; i++) {
      acquire(cb).complete(false);
    }
  }

  /**
   * Returns a breaker that has just tripped.
   */
  private ConcurrentCircuitBreaker openBreaker() {
    ConcurrentCircuitBreaker cb = newBreaker();
    fail(cb, VOLUME_THRESHOLD);
    assertEquals(State.OPEN, cb.state());
    return cb;
  }

  @Test
  void rejectsDisabledConfig() {
    assertThrows(
      InvalidArgumentException.class,
      () -> newBreaker(CircuitBreakerConfig.enabled(false).build())
    );
  }

  @Test
  void factoryReturnsNoopWhenDisabled() {
    AttemptCircuitBreaker cb = AttemptCircuitBreaker.from(CircuitBreakerConfig.enabled(false).build(), "test");
    assertSame(NoopAttemptCircuitBreaker.INSTANCE, cb);
    assertEquals(State.DISABLED, cb.state());

    for (int i = 0; i < 100; i++) {
      AttemptCircuitBreaker.Permit permit = cb.tryAcquire();
      assertNotNull(permit);
      assertFalse(permit.tracksOutcome());
      permit.complete(false);
    }
    assertEquals(State.DISABLED, cb.state());
  }

  @Test
  void factoryReturnsConcurrentBreakerWhenEnabled() {
    AttemptCircuitBreaker cb = AttemptCircuitBreaker.from(CircuitBreakerConfig.builder().build(), "test");
    assertInstanceOf(ConcurrentCircuitBreaker.class, cb);
    assertEquals(State.CLOSED, cb.state());

    AttemptCircuitBreaker.Permit permit = cb.tryAcquire();
    assertNotNull(permit);
    assertTrue(permit.tracksOutcome());
  }

  @Test
  void startsClosed() {
    ConcurrentCircuitBreaker cb = newBreaker();
    assertEquals(State.CLOSED, cb.state());
    assertFalse(acquire(cb).isProbe());
  }

  @Test
  void opensOnlyOnceVolumeThresholdReached() {
    ConcurrentCircuitBreaker cb = newBreaker();
    fail(cb, VOLUME_THRESHOLD - 1);
    assertEquals(State.CLOSED, cb.state());

    fail(cb, 1);
    assertEquals(State.OPEN, cb.state());
    assertNull(cb.tryAcquire());
  }

  @Test
  void staysClosedBelowErrorThreshold() {
    ConcurrentCircuitBreaker cb = newBreaker();
    for (int i = 0; i < 10; i++) {
      acquire(cb).complete(true);
      acquire(cb).complete(true);
      acquire(cb).complete(false); // 33% failures
    }
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void opensAtErrorThreshold() {
    ConcurrentCircuitBreaker cb = newBreaker();
    acquire(cb).complete(true);
    acquire(cb).complete(true);
    acquire(cb).complete(false);
    assertEquals(State.CLOSED, cb.state());
    acquire(cb).complete(false); // 2 of 4 = 50%
    assertEquals(State.OPEN, cb.state());
  }

  @Test
  void rollingWindowForgetsOldResults() {
    ConcurrentCircuitBreaker cb = newBreaker();
    fail(cb, VOLUME_THRESHOLD - 1);
    advance(ROLLING_WINDOW.plusMillis(1));
    fail(cb, 1);
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void grantsExactlyOneProbeAfterSleepWindow() {
    ConcurrentCircuitBreaker cb = openBreaker();

    advance(SLEEP_WINDOW.minusMillis(1));
    assertNull(cb.tryAcquire());

    advance(Duration.ofMillis(1));
    Permit probe = acquire(cb);
    assertTrue(probe.isProbe());
    assertEquals(State.HALF_OPEN, cb.state());

    assertNull(cb.tryAcquire());
    assertNull(cb.tryAcquire());
  }

  @Test
  void successfulProbeCloses() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    acquire(cb).complete(true);

    assertEquals(State.CLOSED, cb.state());
    assertFalse(acquire(cb).isProbe());

    // Window was reset when the circuit closed.
    fail(cb, VOLUME_THRESHOLD - 1);
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void failedProbeReopensAndRestartsSleepWindow() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);
    advance(Duration.ofSeconds(2));
    probe.complete(false);

    assertEquals(State.OPEN, cb.state());
    advance(SLEEP_WINDOW.minusMillis(1));
    assertNull(cb.tryAcquire());
    advance(Duration.ofMillis(1));
    assertTrue(acquire(cb).isProbe());
  }

  @Test
  void staleFailuresDoNotExtendSleepWindow() {
    ConcurrentCircuitBreaker cb = newBreaker();
    List<Permit> inFlight = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      inFlight.add(acquire(cb));
    }
    fail(cb, VOLUME_THRESHOLD);
    assertEquals(State.OPEN, cb.state());

    // Requests dispatched before the circuit opened time out while it is open.
    advance(Duration.ofSeconds(4));
    inFlight.forEach(p -> p.complete(false));

    advance(Duration.ofSeconds(1)); // 5s after opening, only 1s after the stale failures
    assertTrue(acquire(cb).isProbe());
  }

  @Test
  void staleResultsDoNotDecideProbe() {
    ConcurrentCircuitBreaker cb = newBreaker();
    Permit oldSuccess = acquire(cb);
    Permit oldFailure = acquire(cb);
    fail(cb, VOLUME_THRESHOLD);

    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);

    oldSuccess.complete(true);
    assertEquals(State.HALF_OPEN, cb.state(), "stale success must not close the circuit");
    oldFailure.complete(false);
    assertEquals(State.HALF_OPEN, cb.state(), "stale failure must not re-open the circuit");

    probe.complete(true);
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void releasedProbeLetsNextRequestProbe() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);
    assertNull(cb.tryAcquire());

    probe.release();
    assertEquals(State.HALF_OPEN, cb.state());
    assertTrue(acquire(cb).isProbe());
  }

  @Test
  void outstandingProbeBlocksOthersUntilResolved() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);

    advance(SLEEP_WINDOW.multipliedBy(10));
    assertNull(cb.tryAcquire());
    assertEquals(State.HALF_OPEN, cb.state());

    probe.complete(true);
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void permitOutcomeCountsOnlyOnce() {
    ConcurrentCircuitBreaker cb = newBreaker();
    Permit p = acquire(cb);
    for (int i = 0; i < VOLUME_THRESHOLD * 2; i++) {
      p.complete(false);
    }
    p.release();
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void releasedPermitIsNotCounted() {
    ConcurrentCircuitBreaker cb = newBreaker();
    for (int i = 0; i < VOLUME_THRESHOLD * 2; i++) {
      Permit p = acquire(cb);
      p.release();
      p.complete(false);
    }
    assertEquals(State.CLOSED, cb.state());
  }

  @Test
  void concurrentAcquireGrantsSingleProbe() throws Exception {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);

    int threads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Permit>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        results.add(executor.submit(() -> {
          start.await();
          return cb.tryAcquire();
        }));
      }
      start.countDown();

      Permit probe = null;
      for (Future<Permit> f : results) {
        Permit p = f.get(10, TimeUnit.SECONDS);
        if (p != null) {
          assertNull(probe, "more than one permit granted");
          probe = p;
        }
      }
      assertNotNull(probe);
      assertTrue(probe.isProbe());
      assertSame(State.HALF_OPEN, cb.state());

    } finally {
      executor.shutdownNow();
    }
  }

  // ---- Transition logging ----

  @Test
  void logsWhenCircuitOpens() {
    ConcurrentCircuitBreaker cb = newBreaker();
    acquire(cb).complete(true);
    acquire(cb).complete(false);
    acquire(cb).complete(true);
    assertTrue(transitions.isEmpty(), "no transition yet: " + transitions);

    acquire(cb).complete(false); // 2 of 4 = 50%

    String message = onlyTransition();
    assertTrue(message.contains("is now open"), message);
    assertTrue(message.contains("2 of the last 4 requests failed"), message);
    assertTrue(message.contains("threshold of 50%"), message);
    assertTrue(message.contains("Rejecting requests for 5s"), message);
    assertTrue(message.contains(NAME), message);
  }

  @Test
  void logsHalfOpenOnceWhenProbeIsSent() {
    ConcurrentCircuitBreaker cb = openBreaker();
    transitions.clear();

    assertNull(cb.tryAcquire()); // still sleeping
    assertTrue(transitions.isEmpty());

    advance(SLEEP_WINDOW);
    acquire(cb);
    assertTrue(onlyTransition().contains("is now half-open"));

    assertNull(cb.tryAcquire()); // rejected while the probe is outstanding; not a transition
    assertEquals(1, transitions.size());
  }

  @Test
  void logsWhenProbeSucceeds() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);
    transitions.clear();

    probe.complete(true);
    assertTrue(onlyTransition().contains("is now closed: the probe request succeeded"));
  }

  @Test
  void logsWhenProbeFails() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);
    transitions.clear();

    probe.complete(false);
    String message = onlyTransition();
    assertTrue(message.contains("is now open again: the probe request failed"), message);
    assertTrue(message.contains("Rejecting requests for 5s"), message);
  }

  @Test
  void doesNotLogStaleResultsOrReleasedProbes() {
    ConcurrentCircuitBreaker cb = newBreaker();
    Permit stale = acquire(cb);
    fail(cb, VOLUME_THRESHOLD);
    advance(SLEEP_WINDOW);
    Permit probe = acquire(cb);
    transitions.clear();

    stale.complete(false);
    probe.release();
    acquire(cb); // the next probe; still half-open, so not a transition

    assertTrue(transitions.isEmpty(), "unexpected: " + transitions);
  }

  @Test
  void logsOutsideTheLock() {
    ConcurrentCircuitBreaker cb = openBreaker();
    advance(SLEEP_WINDOW);
    acquire(cb).complete(true);
    fail(cb, VOLUME_THRESHOLD);

    assertTrue(transitions.size() >= 4, "expected several transitions: " + transitions);
    assertFalse(loggedWhileHoldingLock, "transitions should be logged after releasing the lock");
  }

  @Test
  void durationsAreFormattedReadably() {
    ConcurrentCircuitBreaker cb = newBreaker(CircuitBreakerConfig.builder()
      .volumeThreshold(1)
      .errorThresholdPercentage(50)
      .sleepWindow(Duration.ofMillis(1500))
      .rollingWindow(Duration.ofSeconds(30))
      .build());
    fail(cb, 1);

    String message = onlyTransition();
    assertTrue(message.contains("within 30s"), message);
    assertTrue(message.contains("Rejecting requests for 1500ms"), message);
  }

  private String onlyTransition() {
    assertEquals(1, transitions.size(), "expected exactly one transition, but got: " + transitions);
    return transitions.get(0);
  }
}
