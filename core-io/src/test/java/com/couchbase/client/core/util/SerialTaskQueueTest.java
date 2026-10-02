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

package com.couchbase.client.core.util;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerialTaskQueueTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static Scheduler scheduler;

  @BeforeAll
  static void beforeAll() {
    scheduler = Schedulers.newParallel("serial-task-queue-test", 4);
  }

  @AfterAll
  static void afterAll() {
    scheduler.dispose();
  }

  @Test
  void tasksNeverOverlap() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);
    AtomicInteger running = new AtomicInteger();
    AtomicInteger maxRunning = new AtomicInteger();

    Flux.range(0, 100)
      .flatMap(i -> queue.submit(() -> Mono.defer(() -> {
          maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
          return Mono.delay(Duration.ofMillis(1), scheduler)
            .doOnTerminate(running::decrementAndGet)
            .then();
        }))
        .subscribeOn(Schedulers.parallel()))
      .then()
      .block(TIMEOUT);

    assertEquals(1, maxRunning.get());
  }

  @Test
  void runsInSubmissionOrder() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);
    List<Integer> order = new CopyOnWriteArrayList<>();

    Flux.range(0, 20)
      .concatMap(i -> Mono.fromRunnable(() ->
        queue.submit(() -> Mono.fromRunnable(() -> order.add(i))).subscribe()))
      .blockLast(TIMEOUT);

    queue.submit(Mono::empty).block(TIMEOUT);

    assertEquals(20, order.size());
    for (int i = 0; i < order.size(); i++) {
      assertEquals(i, order.get(i));
    }
  }

  @Test
  void errorIsPropagatedAndQueueKeepsGoing() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);

    assertThrows(IllegalStateException.class, () ->
      queue.submit(() -> Mono.error(new IllegalStateException())).block(TIMEOUT));

    assertThrows(IllegalStateException.class, () ->
      queue.submit(() -> {
        throw new IllegalStateException();
      }).block(TIMEOUT));

    queue.submit(Mono::empty).block(TIMEOUT);
  }

  @Test
  void cancelledTaskIsSkippedIfNotStarted() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);
    Sinks.Empty<Void> blocker = Sinks.empty();
    AtomicBoolean skippedTaskRan = new AtomicBoolean();

    queue.submit(blocker::asMono).subscribe();
    Disposable cancelled = queue.submit(() -> Mono.fromRunnable(() -> skippedTaskRan.set(true))).subscribe();
    cancelled.dispose();
    blocker.tryEmitEmpty();

    queue.submit(Mono::empty).block(TIMEOUT);
    assertFalse(skippedTaskRan.get());
  }

  @Test
  void callerContinuationMaySubmitAnotherTask() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);

    queue.submit(Mono::empty)
      .then(queue.submit(Mono::empty))
      .block(TIMEOUT);
  }

  @Test
  void requireInTaskPassesInsideTaskEvenAfterThreadHop() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);

    queue.submit(() -> Mono.delay(Duration.ofMillis(1), Schedulers.boundedElastic())
        .thenMany(Flux.range(0, 3))
        .flatMap(i -> queue.requireInTask())
        .then())
      .block(TIMEOUT);
  }

  @Test
  void requireInTaskFailsOutsideTask() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);

    assertThrows(IllegalStateException.class, () -> queue.requireInTask().block(TIMEOUT));
  }

  @Test
  void requireInTaskFailsInTaskOfOtherQueue() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);
    SerialTaskQueue other = new SerialTaskQueue(scheduler);

    assertThrows(IllegalStateException.class, () -> other.submit(queue::requireInTask).block(TIMEOUT));
  }

  @Test
  void requireInTaskFailsInDetachedSubscription() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);
    Sinks.Empty<Void> result = Sinks.empty();

    queue.submit(() -> Mono.fromRunnable(() ->
        queue.requireInTask().subscribe(null, result::tryEmitError, result::tryEmitEmpty)))
      .block(TIMEOUT);

    assertThrows(IllegalStateException.class, () -> result.asMono().block(TIMEOUT));
  }

  @Test
  void waitingForAnotherTaskFromInsideTaskFailsInsteadOfDeadlocking() {
    SerialTaskQueue queue = new SerialTaskQueue(scheduler);

    IllegalStateException e = assertThrows(IllegalStateException.class, () ->
      queue.submit(() -> queue.submit(Mono::empty)).block(TIMEOUT));
    assertTrue(e.getMessage().contains("deadlock"));

    // queue is still usable
    queue.submit(Mono::empty).block(TIMEOUT);
  }

  @Test
  void submitFailsIfSchedulerRejects() {
    Scheduler disposed = Schedulers.newSingle("serial-task-queue-test-disposed");
    disposed.dispose();
    SerialTaskQueue queue = new SerialTaskQueue(disposed);

    assertThrows(RejectedExecutionException.class, () -> queue.submit(Mono::empty).block(TIMEOUT));

    // Each later submit tries again, and fails the same way instead of hanging.
    assertThrows(RejectedExecutionException.class, () -> queue.submit(Mono::empty).block(TIMEOUT));
  }

  @Test
  void queuedTasksFailIfSchedulerRejectsAfterRunningTaskCompletes() {
    Scheduler single = Schedulers.newSingle("serial-task-queue-test-single");
    try {
      SerialTaskQueue queue = new SerialTaskQueue(single);
      Sinks.Empty<Void> blocker = Sinks.empty();
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean queuedTaskRan = new AtomicBoolean();

      Mono<Void> running = queue.submit(() -> blocker.asMono().doOnSubscribe(s -> started.set(true))).cache();
      running.subscribe(null, e -> {});
      Mono<Void> queued = queue.submit(() -> Mono.fromRunnable(() -> queuedTaskRan.set(true))).cache();
      queued.subscribe(null, e -> {});

      waitUntilCondition(started::get, TIMEOUT);
      single.dispose();
      blocker.tryEmitEmpty();

      // The running task's caller still sees its outcome.
      running.block(TIMEOUT);
      // The queued task fails instead of waiting forever.
      assertThrows(RejectedExecutionException.class, () -> queued.block(TIMEOUT));
      assertFalse(queuedTaskRan.get());
    } finally {
      single.dispose();
    }
  }

  @Test
  void runningTaskErrorIsReportedEvenIfSchedulerRejects() {
    Scheduler single = Schedulers.newSingle("serial-task-queue-test-single");
    try {
      SerialTaskQueue queue = new SerialTaskQueue(single);
      Sinks.Empty<Void> blocker = Sinks.empty();
      AtomicBoolean started = new AtomicBoolean();

      Mono<Void> running = queue.submit(() -> blocker.asMono().doOnSubscribe(s -> started.set(true))).cache();
      running.subscribe(null, e -> {});

      waitUntilCondition(started::get, TIMEOUT);
      single.dispose();
      blocker.tryEmitError(new IllegalArgumentException());

      assertThrows(IllegalArgumentException.class, () -> running.block(TIMEOUT));
    } finally {
      single.dispose();
    }
  }
}
