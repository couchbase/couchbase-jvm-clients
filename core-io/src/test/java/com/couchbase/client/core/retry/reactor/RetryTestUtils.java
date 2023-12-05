/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.retry.reactor;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetryTestUtils {

  static void assertDelays(Queue<? extends IterationContext<?>> retries, Long... delayMs) {
    assertEquals(delayMs.length, retries.size());
    int index = 0;
    for (IterationContext<?> repeatContext : retries) {
      assertEquals(delayMs[index].longValue(), repeatContext.backoff().toMillis());
      index++;
    }
  }

  static void assertRandomDelays(Queue<? extends IterationContext<?>> retries, int firstMs, int maxMs) {
    long prevMs = 0;
    int randomValues = 0;
    for (IterationContext<?> context : retries) {
      long backoffMs = context.backoff().toMillis();
      assertTrue(backoffMs >= firstMs && backoffMs <= maxMs, "Unexpected delay " + backoffMs);
      if (backoffMs != firstMs && backoffMs != prevMs)
        randomValues++;
      prevMs = backoffMs;
    }
    assertTrue(randomValues >= 2, "Delays not random"); // Allow for at most one edge case.
  }

  static <T> void testReuseInParallel(int threads, int iterations,
                                      Function<Backoff, Function<Flux<T>, Publisher<Long>>> retryOrRepeat,
                                      Consumer<Function<Flux<T>, Publisher<Long>>> testTask) throws Exception {
    int repeatCount = iterations - 1;
    AtomicInteger nextBackoff = new AtomicInteger();
    // Keep track of the number of backoff invocations per instance
    ConcurrentHashMap<Long, Integer> backoffCounts = new ConcurrentHashMap<>();
    // Use a countdown latch to get all instances to stop in the first backoff callback
    CountDownLatch latch = new CountDownLatch(threads);
    Backoff customBackoff = new Backoff() {
      @Override
      public BackoffDelay apply(IterationContext<?> context) {
        Duration backoff = context.backoff();
        if (latch.getCount() > 0) {
          assertNull(backoff, "Wrong context, backoff must be null");
          backoff = Duration.ofMillis(nextBackoff.incrementAndGet());
          backoffCounts.put(backoff.toMillis(), 1);
          latch.countDown();
          try {
            latch.await(10, TimeUnit.SECONDS);
          }
          catch (Exception e) {
            // ignore, errors are handled later
          }
        } else {
          assertNotNull(backoff, "Wrong context, backoff must not be null");
          long index = backoff.toMillis();
          backoffCounts.put(index, backoffCounts.get(index) + 1);
        }
        return new BackoffDelay(backoff);
      }
    };
    Function<Flux<T>, Publisher<Long>> retryFunc = retryOrRepeat.apply(customBackoff);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < threads; i++) {
        Runnable runnable = () -> testTask.accept(retryFunc);
        futures.add(executor.submit(runnable));
      }
      for (Future<?> future : futures)
        future.get(5, TimeUnit.SECONDS);
    }
    finally {
      executor.shutdownNow();
    }

    assertEquals(0, latch.getCount());
    assertEquals(threads, backoffCounts.size());
    for (Integer count : backoffCounts.values()) {
      //backoff not invoked anymore when maxIteration reached
      assertEquals(repeatCount, count.intValue());
    }
  }
}
