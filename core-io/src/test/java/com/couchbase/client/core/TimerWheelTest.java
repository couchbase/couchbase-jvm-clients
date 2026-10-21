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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Timeout(30)
class TimerWheelTest {

  private final List<TimerWheel> wheels = new ArrayList<>();

  private TimerWheel newWheel(Duration tick, int buckets) {
    TimerWheel wheel = new TimerWheel(tick, buckets, "test-timer-wheel");
    wheels.add(wheel);
    wheel.start();
    return wheel;
  }

  @AfterEach
  void teardown() {
    wheels.forEach(TimerWheel::stop);
  }

  @Test
  void runsTaskAfterItsDelayNeverBefore() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(10), 8);
    CountDownLatch ran = new CountDownLatch(1);
    AtomicLong ranAfterNanos = new AtomicLong();
    AtomicReference<Thread> ranOn = new AtomicReference<>();

    long start = System.nanoTime();
    wheel.newTimeout(() -> {
      ranAfterNanos.set(System.nanoTime() - start);
      ranOn.set(Thread.currentThread());
      ran.countDown();
    }, 50, TimeUnit.MILLISECONDS);

    assertTrue(ran.await(5, TimeUnit.SECONDS));
    assertTrue(ranAfterNanos.get() >= TimeUnit.MILLISECONDS.toNanos(50), "ran early: " + ranAfterNanos.get());
    assertEquals("test-timer-wheel", ranOn.get().getName());
    assertTrue(ranOn.get().isDaemon());
  }

  @Test
  void runsTasksDueInLaterRoundsOfTheWheel() throws Exception {
    // 4 buckets of 1 ms: a 30 ms delay is several rounds of the wheel away.
    TimerWheel wheel = newWheel(Duration.ofMillis(1), 4);
    CountDownLatch ran = new CountDownLatch(1);
    AtomicLong ranAfterNanos = new AtomicLong();

    long start = System.nanoTime();
    wheel.newTimeout(() -> {
      ranAfterNanos.set(System.nanoTime() - start);
      ran.countDown();
    }, 30, TimeUnit.MILLISECONDS);

    assertTrue(ran.await(5, TimeUnit.SECONDS));
    assertTrue(ranAfterNanos.get() >= TimeUnit.MILLISECONDS.toNanos(30), "ran early: " + ranAfterNanos.get());
  }

  @Test
  void runsTaskWithNoDelayAtTheNextTick() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(10), 8);
    CountDownLatch ran = new CountDownLatch(2);
    wheel.newTimeout(ran::countDown, 0, TimeUnit.MILLISECONDS);
    wheel.newTimeout(ran::countDown, -5, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));
  }

  @Test
  void hugeDelaysNeverRun() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    AtomicInteger runs = new AtomicInteger();
    Thread.sleep(10); // so the wheel's age, added to these delays, overflows
    // These would overflow the deadline, and look overdue.
    Runnable saturated = runs::incrementAndGet;
    Runnable maxNanos = runs::incrementAndGet;
    wheel.newTimeout(saturated, Long.MAX_VALUE, TimeUnit.MILLISECONDS); // toNanos saturates at Long.MAX_VALUE
    wheel.newTimeout(maxNanos, Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    // A later task runs, so the others had their chance.
    CountDownLatch later = new CountDownLatch(1);
    wheel.newTimeout(later::countDown, 20, TimeUnit.MILLISECONDS);
    assertTrue(later.await(5, TimeUnit.SECONDS));
    assertEquals(0, runs.get());

    Set<Runnable> unprocessed = wheel.stop().stream().map(TimerWheel.Entry::task).collect(Collectors.toSet());
    assertTrue(unprocessed.contains(saturated) && unprocessed.contains(maxNanos), "still scheduled");
  }

  @Test
  void runsTasksInDeadlineOrder() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 16);
    List<Integer> order = new ArrayList<>(); // only touched by the wheel's thread
    CountDownLatch ran = new CountDownLatch(3);
    wheel.newTimeout(() -> { order.add(3); ran.countDown(); }, 90, TimeUnit.MILLISECONDS);
    wheel.newTimeout(() -> { order.add(1); ran.countDown(); }, 10, TimeUnit.MILLISECONDS);
    wheel.newTimeout(() -> { order.add(2); ran.countDown(); }, 50, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));
    synchronized (order) {
      assertEquals(listOf(1, 2, 3), order);
    }
  }

  @Test
  void cancelledTaskDoesNotRun() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    AtomicInteger runs = new AtomicInteger();
    Timer.Registration registration = wheel.newTimeout(runs::incrementAndGet, 30, TimeUnit.MILLISECONDS);

    assertTrue(registration.cancel());
    assertFalse(registration.cancel(), "already cancelled");

    // A later task runs, so the cancelled one had its chance.
    CountDownLatch later = new CountDownLatch(1);
    wheel.newTimeout(later::countDown, 60, TimeUnit.MILLISECONDS);
    assertTrue(later.await(5, TimeUnit.SECONDS));
    assertEquals(0, runs.get());
  }

  @Test
  void cancellingATaskThatRanHasNoEffect() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    CountDownLatch ran = new CountDownLatch(1);
    Timer.Registration registration = wheel.newTimeout(ran::countDown, 5, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));
    assertFalse(registration.cancel());
  }

  @Test
  void entryReleasesItsTaskOnceItRunsOrIsCancelled() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    CountDownLatch ran = new CountDownLatch(1);
    TimerWheel.Entry ranEntry = (TimerWheel.Entry) wheel.newTimeout(ran::countDown, 5, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));
    assertEquals(null, ranEntry.task());

    TimerWheel.Entry cancelledEntry = (TimerWheel.Entry) wheel.newTimeout(() -> { }, 1, TimeUnit.MINUTES);
    assertTrue(cancelledEntry.task() != null, "not yet run or cancelled");
    cancelledEntry.cancel();
    assertEquals(null, cancelledEntry.task());
  }

  @Test
  void taskThatThrowsDoesNotStopTheWheel() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    wheel.newTimeout(() -> {
      throw new RuntimeException("simulated task failure");
    }, 5, TimeUnit.MILLISECONDS);

    CountDownLatch later = new CountDownLatch(1);
    wheel.newTimeout(later::countDown, 30, TimeUnit.MILLISECONDS);
    assertTrue(later.await(5, TimeUnit.SECONDS));
  }

  @Test
  void stopReturnsTasksThatDidNotRunAndWereNotCancelled() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    Runnable dueLater = () -> { };
    Runnable cancelledTask = () -> { };
    wheel.newTimeout(dueLater, 1, TimeUnit.HOURS);
    wheel.newTimeout(cancelledTask, 1, TimeUnit.HOURS).cancel();

    // And one that ran.
    CountDownLatch ran = new CountDownLatch(1);
    Runnable ranTask = ran::countDown;
    wheel.newTimeout(ranTask, 5, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));

    Set<TimerWheel.Entry> unprocessed = wheel.stop();
    assertEquals(listOf(dueLater), unprocessed.stream().map(TimerWheel.Entry::task).collect(Collectors.toList()));
  }

  @Test
  void stopReturnsTasksNotYetMovedIntoTheWheel() {
    // A long tick, so the task is still waiting to be moved into its bucket when the wheel stops.
    TimerWheel wheel = newWheel(Duration.ofHours(1), 8);
    Runnable task = () -> { };
    wheel.newTimeout(task, 1, TimeUnit.HOURS);

    Set<TimerWheel.Entry> unprocessed = wheel.stop();
    assertEquals(listOf(task), unprocessed.stream().map(TimerWheel.Entry::task).collect(Collectors.toList()));
  }

  @Test
  void stoppedWheelRefusesNewTasks() {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    wheel.stop();
    assertThrows(IllegalStateException.class, () -> wheel.newTimeout(() -> { }, 1, TimeUnit.MILLISECONDS));
    assertThrows(IllegalStateException.class, wheel::start);
    assertTrue(wheel.stop().isEmpty(), "stopping again has no effect");
  }

  @Test
  void cannotStopFromATask() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    CountDownLatch ran = new CountDownLatch(1);
    wheel.newTimeout(() -> {
      try {
        wheel.stop();
      } catch (Throwable t) {
        thrown.set(t);
      }
      ran.countDown();
    }, 5, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));
    assertTrue(thrown.get() instanceof IllegalStateException, String.valueOf(thrown.get()));
  }

  @Test
  void newTimeoutStartsTheWheel() throws Exception {
    TimerWheel wheel = new TimerWheel(Duration.ofMillis(5), 8, "test-timer-wheel");
    wheels.add(wheel);
    CountDownLatch ran = new CountDownLatch(1);
    wheel.newTimeout(ran::countDown, 5, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));
  }

  @Test
  void rejectsInvalidSettings() {
    assertThrows(IllegalArgumentException.class, () -> new TimerWheel(Duration.ZERO, 8, "x"));
    assertThrows(IllegalArgumentException.class, () -> new TimerWheel(Duration.ofMillis(-1), 8, "x"));
    assertThrows(IllegalArgumentException.class, () -> new TimerWheel(Duration.ofMillis(10), 0, "x"));
  }

  @Test
  void roundsBucketsUpToAPowerOfTwoAndTickUpToAMillisecond() {
    String description = new TimerWheel(Duration.ofNanos(10), 5, "x").toString();
    assertTrue(description.contains("buckets=8"), description);
    assertTrue(description.contains("tickNanos=1000000"), description);
  }

  @Test
  void runsManyTasksScheduledConcurrently() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(1), 64);
    int threads = 8;
    int perThread = 10_000;
    CountDownLatch ran = new CountDownLatch(threads * perThread);
    AtomicInteger cancelledRuns = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int t = 0; t < threads; t++) {
        executor.execute(() -> {
          for (int i = 0; i < perThread; i++) {
            wheel.newTimeout(ran::countDown, i % 50, TimeUnit.MILLISECONDS);
            // And one cancelled right away, like a request that completes before its timeout.
            wheel.newTimeout(cancelledRuns::incrementAndGet, 20, TimeUnit.MILLISECONDS).cancel();
          }
        });
      }
      assertTrue(ran.await(20, TimeUnit.SECONDS), "remaining: " + ran.getCount());
      Thread.sleep(50); // past the cancelled tasks' delay
      assertEquals(0, cancelledRuns.get());
    } finally {
      executor.shutdownNow();
    }
  }

  private static final class Node {
    static final AtomicReferenceFieldUpdater<Node, Node> NEXT = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

    final int producer;
    final int sequence;
    volatile Node next;

    Node(int producer, int sequence) {
      this.producer = producer;
      this.sequence = sequence;
    }
  }

  @Test
  void linkedQueuePollsEveryItemOnceInTheOrderEachThreadAddedThem() throws Exception {
    TimerWheel.LinkedQueue<Node> queue = new TimerWheel.LinkedQueue<>(Node.NEXT, new Node(-1, -1));
    int threads = 8;
    int perThread = 100_000;
    CountDownLatch go = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int t = 0; t < threads; t++) {
        int producer = t;
        executor.execute(() -> {
          try {
            go.await();
          } catch (InterruptedException e) {
            return;
          }
          for (int i = 0; i < perThread; i++) {
            queue.add(new Node(producer, i));
          }
        });
      }
      go.countDown();

      int[] nextExpected = new int[threads];
      Node previous = null;
      for (int polled = 0; polled < threads * perThread; ) {
        Node node = queue.poll();
        if (node == null) {
          Thread.yield();
          continue;
        }
        assertEquals(nextExpected[node.producer]++, node.sequence, "producer " + node.producer);
        if (previous != null) {
          assertEquals(null, previous.next, "a polled item no longer links to the next");
        }
        previous = node;
        polled++;
      }
      assertEquals(null, queue.poll());
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Links nodes like {@link Node#NEXT}, but the add() of one node pauses between its two steps until released.
   */
  private static final class PausingLink extends AtomicReferenceFieldUpdater<Node, Node> {
    final Node pauseOn;
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    PausingLink(Node pauseOn) {
      this.pauseOn = pauseOn;
    }

    @Override
    public void lazySet(Node obj, Node newValue) {
      if (newValue == pauseOn) {
        paused.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      Node.NEXT.lazySet(obj, newValue);
    }

    @Override
    public boolean compareAndSet(Node obj, Node expect, Node update) {
      return Node.NEXT.compareAndSet(obj, expect, update);
    }

    @Override
    public boolean weakCompareAndSet(Node obj, Node expect, Node update) {
      return Node.NEXT.weakCompareAndSet(obj, expect, update);
    }

    @Override
    public void set(Node obj, Node newValue) {
      Node.NEXT.set(obj, newValue);
    }

    @Override
    public Node get(Node obj) {
      return Node.NEXT.get(obj);
    }
  }

  @Test
  void linkedQueuePollWaitingForAddsWaitsOutAnAddInProgress() throws Exception {
    Node first = new Node(0, 0);
    Node second = new Node(1, 0);
    PausingLink link = new PausingLink(first);
    TimerWheel.LinkedQueue<Node> queue = new TimerWheel.LinkedQueue<>(link, new Node(-1, -1));

    Thread adder = new Thread(() -> queue.add(first));
    adder.start();
    assertTrue(link.paused.await(5, TimeUnit.SECONDS));
    queue.add(second); // returns, but is behind the add() in progress

    assertEquals(null, queue.poll(), "poll() stops at the add() in progress");

    Thread releaser = new Thread(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        return;
      }
      link.release.countDown();
    });
    releaser.start();

    assertEquals(first, queue.pollWaitingForAdds());
    assertEquals(second, queue.pollWaitingForAdds());
    assertEquals(null, queue.pollWaitingForAdds(), "empty");
    adder.join();
    releaser.join();
  }

  @Test
  void taskThatLeavesItsThreadInterruptedDoesNotAffectTheNextTask() throws Exception {
    TimerWheel wheel = newWheel(Duration.ofMillis(5), 8);
    CountDownLatch ran = new CountDownLatch(1);
    AtomicReference<Boolean> nextSawInterrupt = new AtomicReference<>();
    // Due at the same tick, so they run one after the other, in the order scheduled.
    wheel.newTimeout(() -> Thread.currentThread().interrupt(), 5, TimeUnit.MILLISECONDS);
    wheel.newTimeout(() -> {
      nextSawInterrupt.set(Thread.currentThread().isInterrupted());
      ran.countDown();
    }, 5, TimeUnit.MILLISECONDS);

    assertTrue(ran.await(5, TimeUnit.SECONDS));
    assertEquals(false, nextSawInterrupt.get());
  }

  @Test
  void interruptingTheTimerThreadDoesNotMakeItSpin() throws Exception {
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    assumeTrue(threads.isThreadCpuTimeSupported());

    TimerWheel wheel = newWheel(Duration.ofMillis(10), 8);
    AtomicReference<Thread> timerThread = new AtomicReference<>();
    CountDownLatch ran = new CountDownLatch(1);
    wheel.newTimeout(() -> {
      timerThread.set(Thread.currentThread());
      ran.countDown();
    }, 1, TimeUnit.MILLISECONDS);
    assertTrue(ran.await(5, TimeUnit.SECONDS));

    timerThread.get().interrupt();
    long cpuBefore = threads.getThreadCpuTime(timerThread.get().getId());
    Thread.sleep(500);
    long cpuMillis = TimeUnit.NANOSECONDS.toMillis(threads.getThreadCpuTime(timerThread.get().getId()) - cpuBefore);
    assertTrue(cpuMillis < 250, "timer thread used " + cpuMillis + " ms of CPU in 500 ms");

    // And it still runs tasks.
    CountDownLatch ranAgain = new CountDownLatch(1);
    wheel.newTimeout(ranAgain::countDown, 1, TimeUnit.MILLISECONDS);
    assertTrue(ranAgain.await(5, TimeUnit.SECONDS));
  }

  @SafeVarargs
  private static <T> List<T> listOf(T... items) {
    List<T> result = new ArrayList<>();
    for (T item : items) {
      result.add(item);
    }
    return result;
  }
}
