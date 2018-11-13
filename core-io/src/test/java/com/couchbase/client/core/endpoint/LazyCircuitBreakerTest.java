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

import com.couchbase.client.core.msg.Response;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link LazyCircuitBreaker}.
 *
 * @since 2.0.0
 */
class LazyCircuitBreakerTest {

  @Test
  void failsIfDisabled() {
    assertThrows(
      IllegalArgumentException.class,
      () -> new LazyCircuitBreaker(CircuitBreakerConfig.disabled())
    );
  }

  /**
   * Make sure the circuit breaker starts closed.
   */
  @Test
  void startsClosed() {
    LazyCircuitBreaker cb = new LazyCircuitBreaker(CircuitBreakerConfig.create());
    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
  }

  /**
   * There is a volume threshold as a sanity cap, so make sure that even if all
   * fail it only starts to open after the volume is reached.
   */
  @Test
  void opensOverVolumeThreshold() {
    CircuitBreakerConfig config = CircuitBreakerConfig.create();
    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    for (int i = 0; i < config.volumeThreshold() - 1; i++) {
      CompletableFuture<Response> future = new CompletableFuture<>();
      cb.track(future);
      future.completeExceptionally(new Exception());
      assertEquals(CircuitBreaker.State.CLOSED, cb.state());
      assertTrue(cb.allowsRequest());
    }

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());
    assertEquals(CircuitBreaker.State.OPEN, cb.state());
    assertFalse(cb.allowsRequest());
  }

  /**
   * Once over the volume, make sure to only open when the error percentage threshold
   * is reached even if good and failed responses are mixed.
   */
  @Test
  void opensOverErrorThreshold() {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .errorThresholdPercentage(80)
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    // Make good 100 requests to add some seeds
    for (int i = 0; i < 100; i++) {
      CompletableFuture<Response> future = new CompletableFuture<>();
      cb.track(future);
      future.complete(mock(Response.class));
      assertTrue(cb.allowsRequest());
      assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    }

    // now make a good bunch of requests to crank up the ratio (so 400 failed
    // divided by 500 total is 0,8 -> 80% ratio).. minus one so the next one
    // will trip it below.
    for (int i = 0; i < 399; i++) {
      CompletableFuture<Response> future = new CompletableFuture<>();
      cb.track(future);
      future.completeExceptionally(new Exception());
      assertTrue(cb.allowsRequest());
      assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    }

    // This one trips it.
    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());
    assertFalse(cb.allowsRequest());
    assertEquals(CircuitBreaker.State.OPEN, cb.state());
  }

  /**
   * Makes sure that after the configued sleep time, a new request is allowed to
   * go in as a canary.
   */
  @Test
  void allowsCanaryAfterSleepTime() throws InterruptedException {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .volumeThreshold(1)
      .sleepWindow(Duration.ofMillis(50))
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);
    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());
    assertFalse(cb.allowsRequest());
    assertEquals(CircuitBreaker.State.OPEN, cb.state());

    Thread.sleep(config.sleepWindow().toMillis() + 1);
    assertEquals(CircuitBreaker.State.OPEN, cb.state());
    assertTrue(cb.allowsRequest());

    future = new CompletableFuture<>();
    cb.track(future);

    assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state());
    assertFalse(cb.allowsRequest());
  }

  /**
   * Make sure that once a good canary happens, the circuit breaker closes again and
   * resets its internal state back to "normal".
   */
  @Test
  void goodCanaryCloses() throws InterruptedException {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .volumeThreshold(1)
      .sleepWindow(Duration.ofMillis(50))
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());

    Thread.sleep(config.sleepWindow().toMillis() + 1);
    future = new CompletableFuture<>();
    cb.track(future);

    assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state());
    assertFalse(cb.allowsRequest());

    future.complete(mock(Response.class));
    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
  }

  /**
   * Make sure that a failed canary gets it back into the open position and resets the
   * sleep timer.
   */
  @Test
  void failedCanaryOpensAgain() throws InterruptedException {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .volumeThreshold(1)
      .sleepWindow(Duration.ofMillis(50))
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());

    Thread.sleep(config.sleepWindow().toMillis() + 1);
    future = new CompletableFuture<>();
    cb.track(future);

    assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state());
    assertFalse(cb.allowsRequest());

    future.completeExceptionally(new Exception());
    assertEquals(CircuitBreaker.State.OPEN, cb.state());
    assertFalse(cb.allowsRequest());

    Thread.sleep(config.sleepWindow().toMillis() + 1);
    assertEquals(CircuitBreaker.State.OPEN, cb.state());
    assertTrue(cb.allowsRequest());
  }

  /**
   * Assert that the caller can always reset from an open state into a closed state.
   */
  @Test
  void canResetFromOpenState() {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .volumeThreshold(1)
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());

    assertEquals(CircuitBreaker.State.OPEN, cb.state());
    assertFalse(cb.allowsRequest());

    cb.reset();

    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
  }

  /**
   * Assert that the caller can always reset from a half open state into a closed state.
   */
  @Test
  void canResetFromHalfOpenState() throws InterruptedException {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .volumeThreshold(1)
      .sleepWindow(Duration.ofMillis(50))
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());

    Thread.sleep(config.sleepWindow().toMillis() + 1);
    future = new CompletableFuture<>();
    cb.track(future);

    assertEquals(CircuitBreaker.State.HALF_OPEN, cb.state());
    assertFalse(cb.allowsRequest());

    cb.reset();

    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
  }

  /**
   * Since we have a rolling window, make sure once we cross over ops are cleaned
   * up properly and do not carry over in the next window.
   */
  @Test
  void cleansUpWhenRolledOver() throws InterruptedException {
    CircuitBreakerConfig config = CircuitBreakerConfig
      .builder()
      .volumeThreshold(2)
      .rollingWindow(Duration.ofMillis(100))
      .build();

    LazyCircuitBreaker cb = new LazyCircuitBreaker(config);

    CompletableFuture<Response> future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());

    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());

    Thread.sleep(config.rollingWindow().toMillis() + 1);

    future = new CompletableFuture<>();
    cb.track(future);
    future.completeExceptionally(new Exception());

    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
  }

  /**
   * Assert that the caller can always reset while being closed, more or less being a noop but
   * it also resets the internal counters.
   */
  @Test
  void canResetFromClosedState() {
    LazyCircuitBreaker cb = new LazyCircuitBreaker(CircuitBreakerConfig.create());
    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
    cb.reset();
    assertEquals(CircuitBreaker.State.CLOSED, cb.state());
    assertTrue(cb.allowsRequest());
  }

}