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

package com.couchbase.client.core;

import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.TimerConfig;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Hammers {@link Timer} from many threads, mainly to check the queues {@link TimerWheel} uses to hand tasks and
 * cancellations to its thread ({@link TimerWheel.LinkedQueue}). A queue that lost, duplicated or reordered items
 * would show up here as a task that never runs, runs twice, runs after it was cancelled, isn't returned when the
 * timer stops, or stays in the wheel after it's cancelled.
 * <p>
 * Each test takes a few seconds. For a longer soak, run with {@code -DtimerStressScale=N} to do N times the work.
 */
@Timeout(120)
class TimerStressTest {

  private static final int SCALE = Integer.getInteger("timerStressScale", 1);
  private static final int THREADS = Math.max(4, Runtime.getRuntime().availableProcessors());
  private static final RuntimeException COMPLETED = new RuntimeException("completed by the test");

  private static Timer newTimer(int numTimers) {
    return Timer.createAndStart(
      Long.MAX_VALUE,
      TimerConfig.builder()
        .numTimers(numTimers)
        .tickDuration(Duration.ofMillis(1))
        // Few buckets, so most tasks wait more than one round of the wheel.
        .numBuckets(16)
        .build()
    );
  }

  /**
   * Many threads schedule tasks as fast as they can (more per tick than the wheel moves in one), and cancel some:
   * at once, never, or from another thread, around when they're due, to race with the timer's thread.
   */
  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  void everyTaskRunsOnceUnlessCancelled(int numTimers) throws Exception {
    int perThread = 50_000 * SCALE;
    int tasks = THREADS * perThread;
    long maxDelayMillis = 50;

    AtomicIntegerArray runs = new AtomicIntegerArray(tasks);
    AtomicIntegerArray ranEarly = new AtomicIntegerArray(tasks);
    // 0 = not cancelled, 1 = cancel() returned true, 2 = cancel() returned false
    AtomicIntegerArray cancelResults = new AtomicIntegerArray(tasks);
    AtomicInteger totalRuns = new AtomicInteger();
    AtomicInteger totalCancelled = new AtomicInteger();

    ConcurrentLinkedQueue<IndexedRegistration> toCancel = new ConcurrentLinkedQueue<>();
    AtomicInteger producersRunning = new AtomicInteger(THREADS);
    AtomicReference<Throwable> failure = new AtomicReference<>();

    Timer timer = newTimer(numTimers);
    try {
      List<Thread> threads = new ArrayList<>();
      CountDownLatch go = new CountDownLatch(1);

      for (int t = 0; t < THREADS; t++) {
        int first = t * perThread;
        threads.add(startThread("producer-" + t, failure, () -> {
          try {
            go.await();
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = first; i < first + perThread; i++) {
              int index = i;
              int choice = random.nextInt(3);
              // Tasks cancelled later get short delays, so the cancel races with the task running.
              long delayNanos = choice == 2
                ? random.nextLong(TimeUnit.MILLISECONDS.toNanos(3))
                : random.nextLong(TimeUnit.MILLISECONDS.toNanos(maxDelayMillis));
              long due = System.nanoTime() + delayNanos;

              Timer.Registration registration = timer.schedule(() -> {
                if (System.nanoTime() < due) {
                  ranEarly.set(index, 1);
                }
                runs.incrementAndGet(index);
                totalRuns.incrementAndGet();
              }, Duration.ofNanos(delayNanos));

              if (registration == null) {
                throw new AssertionError("Timer refused task " + index);
              }
              if (choice == 1) {
                cancel(registration, index, cancelResults, totalCancelled);
              } else if (choice == 2) {
                toCancel.add(new IndexedRegistration(index, registration));
              }
            }
          } finally {
            producersRunning.decrementAndGet(); // even if this producer failed, or the cancellers would wait forever
          }
        }));
      }

      for (int t = 0; t < 2; t++) {
        threads.add(startThread("canceller-" + t, failure, () -> {
          while (true) {
            IndexedRegistration item = toCancel.poll();
            if (item != null) {
              cancel(item.registration, item.index, cancelResults, totalCancelled);
            } else if (producersRunning.get() == 0 && toCancel.isEmpty()) {
              return;
            } else {
              Thread.yield();
            }
          }
        }));
      }

      go.countDown();
      for (Thread thread : threads) {
        thread.join();
      }
      assertNoFailure(failure);

      // Wait for every task that wasn't cancelled to run.
      int expectedRuns = tasks - totalCancelled.get();
      long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (totalRuns.get() < expectedRuns && System.nanoTime() < waitUntil) {
        Thread.sleep(10);
      }
      // And a little longer, to catch extra runs.
      Thread.sleep(maxDelayMillis * 2);

      List<String> problems = new ArrayList<>();
      for (int i = 0; i < tasks && problems.size() < 20; i++) {
        int n = runs.get(i);
        int cancelResult = cancelResults.get(i);
        if (cancelResult == 1 && n != 0) {
          problems.add("task " + i + " ran " + n + " times after it was cancelled");
        } else if (cancelResult != 1 && n != 1) {
          problems.add("task " + i + " ran " + n + " times (cancel result " + cancelResult + ")");
        }
        if (ranEarly.get(i) != 0) {
          problems.add("task " + i + " ran before it was due");
        }
      }
      assertTrue(
        problems.isEmpty(),
        "expected " + expectedRuns + " runs, got " + totalRuns.get() + "; first problems: " + problems
      );

      assertTrue(stopWithin(timer), "Timer.stop() did not return within 10 seconds");
    } finally {
      stopWithin(timer); // in case the test failed before stopping it; a second stop() returns at once
    }
  }

  /**
   * Many threads register requests while the timer stops. Afterwards, no request may be left waiting forever:
   * each one is completed, timed out, or cancelled because the timer stopped. Repeated, since the race window
   * is small.
   */
  @Test
  void stoppingAccountsForEveryRequest() throws Exception {
    int rounds = 50 * SCALE;
    Core core = stubCore();

    for (int round = 0; round < rounds; round++) {
      Timer timer = newTimer(1 + round % 2);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      CountDownLatch go = new CountDownLatch(1);
      AtomicInteger stopped = new AtomicInteger(); // set once stop() has returned
      List<List<CountingRequest>> requestsByThread = new ArrayList<>();
      List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < THREADS; t++) {
        List<CountingRequest> requests = new ArrayList<>();
        requestsByThread.add(requests);
        threads.add(startThread("producer-" + t, failure, () -> {
          go.await();
          ThreadLocalRandom random = ThreadLocalRandom.current();
          // Keep going a little past stop(), to cover registering after it.
          int extra = 100;
          while (stopped.get() == 0 || extra-- > 0) {
            Duration timeout = random.nextBoolean()
              ? Duration.ofNanos(random.nextLong(TimeUnit.MILLISECONDS.toNanos(3)))
              : Duration.ofHours(1);
            CountingRequest request = new CountingRequest(timeout, core);
            requests.add(request);
            register(timer, request);
            if (random.nextInt(4) == 0) {
              request.fail(COMPLETED); // like a response arriving: cancels the timeout task
            }
          }
        }));
      }

      go.countDown();
      Thread.sleep(ThreadLocalRandom.current().nextInt(20));
      try {
        assertTrue(stopWithin(timer), "round " + round + ": Timer.stop() did not return within 10 seconds");
      } finally {
        stopped.set(1);
      }
      for (Thread thread : threads) {
        thread.join();
      }
      assertNoFailure(failure);

      int total = 0;
      for (List<CountingRequest> requests : requestsByThread) {
        for (CountingRequest request : requests) {
          total++;
          String problem = request.problem();
          if (problem != null) {
            fail("round " + round + ": " + problem);
          }
        }
      }
      assertTrue(total > THREADS, "round " + round + " registered only " + total + " requests");
    }
  }

  /**
   * Many threads schedule tasks that aren't due for an hour, and cancel them. The timer must let go of every
   * cancelled task, which it does only once its thread has taken it from the queue of cancellations.
   */
  @Test
  void cancelledTasksAreReleased() throws Exception {
    int perThread = 50_000 * SCALE;
    Timer timer = newTimer(1);
    try {
      AtomicReference<Throwable> failure = new AtomicReference<>();
      List<List<WeakReference<Timer.Registration>>> refsByThread = new ArrayList<>();
      List<Thread> threads = new ArrayList<>();
      CountDownLatch go = new CountDownLatch(1);

      for (int t = 0; t < THREADS; t++) {
        List<WeakReference<Timer.Registration>> refs = new ArrayList<>();
        refsByThread.add(refs);
        threads.add(startThread("producer-" + t, failure, () -> {
          go.await();
          ThreadLocalRandom random = ThreadLocalRandom.current();
          List<Timer.Registration> batch = new ArrayList<>();
          for (int i = 0; i < perThread; i++) {
            Timer.Registration registration = timer.schedule(() -> { }, Duration.ofHours(1));
            if (registration == null) {
              throw new AssertionError("Timer refused a task");
            }
            refs.add(new WeakReference<>(registration));
            batch.add(registration);
            // Cancel in batches of random size, so some are cancelled before the timer's thread moves them into
            // the wheel, and some after.
            if (random.nextInt(100) == 0) {
              cancelAll(batch);
            }
          }
          cancelAll(batch);
        }));
      }

      go.countDown();
      for (Thread thread : threads) {
        thread.join();
      }
      assertNoFailure(failure);

      // Each queue keeps the last item it returned, so its wheel can keep up to two cancelled tasks.
      int allowedRetained = 2;
      int retained = Integer.MAX_VALUE;
      long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < waitUntil) {
        System.gc();
        retained = 0;
        for (List<WeakReference<Timer.Registration>> refs : refsByThread) {
          for (WeakReference<Timer.Registration> ref : refs) {
            if (ref.get() != null) {
              retained++;
            }
          }
        }
        if (retained <= allowedRetained) {
          break;
        }
        Thread.sleep(100);
      }
      assertTrue(
        retained <= allowedRetained,
        retained + " of " + (THREADS * perThread) + " cancelled tasks are still referenced"
      );

      assertTrue(stopWithin(timer), "Timer.stop() did not return within 10 seconds");
    } finally {
      stopWithin(timer); // in case the test failed before stopping it; a second stop() returns at once
    }
  }

  @SuppressWarnings("unchecked")
  private static void register(Timer timer, Request<?> request) {
    timer.register((Request<Response>) request); // register() takes Request<Response>, though it works with any request
  }

  private static void cancel(
    Timer.Registration registration,
    int index,
    AtomicIntegerArray cancelResults,
    AtomicInteger totalCancelled
  ) {
    if (registration.cancel()) {
      cancelResults.set(index, 1);
      totalCancelled.incrementAndGet();
    } else {
      cancelResults.set(index, 2);
    }
  }

  private static void cancelAll(List<Timer.Registration> registrations) {
    for (Timer.Registration registration : registrations) {
      if (!registration.cancel()) {
        throw new AssertionError("A task that wasn't due for an hour couldn't be cancelled");
      }
    }
    registrations.clear();
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static Thread startThread(String name, AtomicReference<Throwable> failure, ThrowingRunnable body) {
    Thread thread = new Thread(() -> {
      try {
        body.run();
      } catch (Throwable t) {
        failure.compareAndSet(null, t);
      }
    }, "timer-stress-" + name);
    thread.setDaemon(true); // so a hung test can't keep the JVM alive
    thread.start();
    return thread;
  }

  /**
   * Stops the timer, giving up after a while. A queue that loses a link can make stop() wait forever, and that
   * should fail the test, not hang it.
   *
   * @return whether it stopped in time
   * @throws AssertionError if stop() threw
   */
  private static boolean stopWithin(Timer timer) throws InterruptedException {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread stopper = startThread("stopper", failure, timer::stop);
    stopper.join(TimeUnit.SECONDS.toMillis(10));
    assertNoFailure(failure);
    return !stopper.isAlive();
  }

  private static void assertNoFailure(AtomicReference<Throwable> failure) {
    Throwable t = failure.get();
    if (t != null) {
      throw new AssertionError("A test thread failed", t);
    }
  }

  /**
   * A core that's just enough for requests to be cancelled. Stub-only, so the mocks don't record the
   * many calls made on them.
   */
  private static Core stubCore() {
    Core core = mock(Core.class, withSettings().stubOnly());
    when(core.appTelemetryCollector()).thenReturn(AppTelemetryCollector.NOOP);
    CoreEnvironment env = mock(CoreEnvironment.class, withSettings().stubOnly());
    CoreContext ctx = new CoreContext(core, 1, env, PasswordAuthenticator.create("username", "password"));
    when(core.context()).thenReturn(ctx);
    return core;
  }

  private static final class IndexedRegistration {
    final int index;
    final Timer.Registration registration;

    IndexedRegistration(int index, Timer.Registration registration) {
      this.index = index;
      this.registration = registration;
    }
  }

  /**
   * A request that counts how the timer cancels it.
   */
  private static final class CountingRequest extends NoopRequest {
    private final AtomicInteger timeouts = new AtomicInteger();
    private final AtomicInteger shutdowns = new AtomicInteger();
    private final AtomicInteger others = new AtomicInteger();
    private volatile long timedOutAt;

    CountingRequest(Duration timeout, Core core) {
      super(timeout, core.context(), BestEffortRetryStrategy.INSTANCE, CollectionIdentifier.fromDefault("bucket"));
    }

    @Override
    public void cancel(CancellationReason reason, Function<Throwable, Throwable> exceptionTranslator) {
      if (reason == CancellationReason.TIMEOUT) {
        timedOutAt = System.nanoTime();
        timeouts.incrementAndGet();
      } else if (reason == CancellationReason.SHUTDOWN) {
        shutdowns.incrementAndGet();
      } else {
        others.incrementAndGet();
      }
      super.cancel(reason, exceptionTranslator);
    }

    /**
     * Returns what's wrong with how the timer treated this request, or null if nothing.
     * Call only once the timer has stopped, and every thread that registered or completed requests has finished.
     */
    String problem() {
      String self = "request " + id() + " (timeout " + timeout() + ", state " + state() + ", timeouts " + timeouts
        + ", shutdowns " + shutdowns + ", others " + others + ")";
      if (!completed()) {
        return self + " was left waiting";
      }
      if (others.get() != 0) {
        return self + " was cancelled for an unexpected reason";
      }
      if (timeouts.get() > 1) {
        return self + " timed out more than once";
      }
      if (timeouts.get() == 1 && timedOutAt < createdAt() + timeout().toNanos()) {
        return self + " timed out early";
      }
      return null;
    }

    private String state() {
      if (succeeded()) {
        return "succeeded";
      }
      return cancelled() ? "cancelled: " + cancellationReason() : (completed() ? "failed" : "incomplete");
    }
  }
}
