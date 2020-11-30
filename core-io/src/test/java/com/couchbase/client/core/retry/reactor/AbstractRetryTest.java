/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.retry.reactor;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class AbstractRetryTest {

  @Test
  void calculateTimeoutUsesScheduler() {
    VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

    AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
      Backoff.ZERO_BACKOFF, Jitter.NO_JITTER, scheduler, null) {
      @Override
      public Publisher<Long> apply(Flux<Integer> integerFlux) {
        return null;
      }
    };

    assertEquals(1000L, abstractRetry.calculateTimeout().toEpochMilli());
    scheduler.advanceTimeBy(Duration.ofSeconds(3));
    assertEquals(4000L, abstractRetry.calculateTimeout().toEpochMilli());
  }

  @Test
  void calculateBackoffUsesScheduler() {
    VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    Instant timeoutInstant = Instant.ofEpochMilli(1000);

    AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
      Backoff.fixed(Duration.ofMillis(600)), Jitter.NO_JITTER, scheduler, null) {
      @Override
      public Publisher<Long> apply(Flux<Integer> integerFlux) {
        return null;
      }
    };

    RetryContext<String> retryContext = new DefaultContext<>(null, 1, BackoffDelay.ZERO, null);

    BackoffDelay backoff = abstractRetry.calculateBackoff(retryContext, timeoutInstant);
    assertNotEquals(backoff, AbstractRetry.RETRY_EXHAUSTED);
    assertEquals(backoff.delay, Duration.ofMillis(600));

    scheduler.advanceTimeBy(Duration.ofMillis(500));

    backoff = abstractRetry.calculateBackoff(retryContext, timeoutInstant);
    assertEquals(backoff, AbstractRetry.RETRY_EXHAUSTED);
  }

  @Test
  void calculateBackoffUsesDefaultClockWhenNoScheduler() {
    Instant timeoutInstant = Instant.now().plusSeconds(3);

    AbstractRetry<String, Integer> abstractRetry = new AbstractRetry<String, Integer>(2, Duration.ofSeconds(1),
      Backoff.fixed(Duration.ofMillis(600)), Jitter.NO_JITTER, null, null) {
      @Override
      public Publisher<Long> apply(Flux<Integer> integerFlux) {
        return null;
      }
    };

    RetryContext<String> retryContext = new DefaultContext<>(null, 1, BackoffDelay.ZERO, null);

    BackoffDelay backoff = abstractRetry.calculateBackoff(retryContext, timeoutInstant);
    assertNotEquals(backoff, AbstractRetry.RETRY_EXHAUSTED);
    assertEquals(backoff.delay, Duration.ofMillis(600));
  }

}
