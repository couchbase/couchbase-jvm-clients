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

import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.util.CbThreads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Timeout(10)
class InterruptWatcherTest {

  private final InterruptWatcher watcher = new InterruptWatcher(
    Duration.ofMillis(10), // check interval
    Duration.ofMillis(50), // park after
    Duration.ofMinutes(1), // exit after
    "test-interrupt-watcher"
  );
  private final Request<?> request = mock(Request.class);

  private final AtomicBoolean release = new AtomicBoolean();
  private final List<Thread> threads = new ArrayList<>();

  @AfterEach
  void teardown() throws InterruptedException {
    release.set(true);
    for (Thread t : threads) {
      t.join();
    }
  }

  /**
   * Starts a platform thread that watches itself (or did, if stopWatchingFirst), then waits until the test ends,
   * ignoring interrupts, like a platform thread blocked reading a socket.
   */
  private Thread startWatchedThread(boolean stopWatchingFirst) throws InterruptedException {
    return startWatchedThread(watcher, request, stopWatchingFirst);
  }

  private Thread startWatchedThread(InterruptWatcher watcher, Request<?> request, boolean stopWatchingFirst)
    throws InterruptedException {
    CountDownLatch ready = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      InterruptWatcher.Registration registration = watcher.watchCurrentThread(request);
      if (stopWatchingFirst) {
        registration.close();
      }
      ready.countDown();
      while (!release.get()) {
        LockSupport.parkNanos(1_000_000); // doesn't clear the interrupt
      }
      if (!stopWatchingFirst) {
        registration.close();
      }
    });
    threads.add(t);
    t.start();
    ready.await();
    return t;
  }

  @Test
  void cancelsTheRequestOfAWatchedThreadThatIsInterrupted() throws Exception {
    startWatchedThread(false).interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
  }

  @Test
  void cancelsOnlyOnce() throws Exception {
    startWatchedThread(false).interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
    Thread.sleep(100);
    verify(request, times(1)).cancel(any(CancellationReason.class));
  }

  @Test
  void leavesAThreadThatIsNotInterruptedAlone() throws Exception {
    startWatchedThread(false);
    Thread.sleep(100);
    verify(request, never()).cancel(any(CancellationReason.class));
  }

  @Test
  void stopsWatchingWhenTheRegistrationIsClosed() throws Exception {
    startWatchedThread(true).interrupt();
    Thread.sleep(100);
    verify(request, never()).cancel(any(CancellationReason.class));
  }

  @Test
  void startsWhenFirstNeededAndParksWhileThereIsNothingToWatch() throws Exception {
    assertNull(watcher.checker(), "shouldn't start a thread until there's something to watch");

    startWatchedThread(true);
    Thread checker = watcher.checker();
    assertNotNull(checker);
    assertNull(checker.getContextClassLoader(), "shouldn't keep the creating code's class loader reachable");
    waitUntilCondition(watcher::parked);

    // Still works after parking.
    startWatchedThread(false).interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
    assertSame(checker, watcher.checker());
  }

  @Test
  void doesNotWakeTheCheckerForEachCallWhileItsStillChecking() {
    // Like a platform thread sending one request after another.
    for (int i = 0; i < 1000; i++) {
      watcher.watchCurrentThread(request).close();
    }
    assertEquals(1, watcher.wakeUps(), "should only start it");
  }

  @Test
  void exitsAfterBeingIdleForAWhileAndRestartsWhenNeeded() throws Exception {
    InterruptWatcher exiting = new InterruptWatcher(
      Duration.ofMillis(10), // check interval
      Duration.ofMillis(20), // park after
      Duration.ofMillis(100), // exit after
      "test-exiting-interrupt-watcher"
    );

    exiting.watchCurrentThread(request).close();
    Thread first = exiting.checker();
    assertNotNull(first);
    first.join(5000);
    assertFalse(first.isAlive(), "should exit after being idle");
    assertNull(exiting.checker());

    // Restarts on the next call.
    CountDownLatch ready = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      try (InterruptWatcher.Registration ignored = exiting.watchCurrentThread(request)) {
        ready.countDown();
        while (!release.get()) {
          LockSupport.parkNanos(1_000_000); // doesn't clear the interrupt
        }
      }
    });
    threads.add(t);
    t.start();
    ready.await();
    Thread second = exiting.checker();
    assertNotNull(second);
    assertNotSame(first, second);

    t.interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
  }

  @Test
  void doesNotWatchVirtualThreads() throws Exception {
    // Interrupting a virtual thread closes the socket it's reading, so it doesn't need watching.
    ThreadFactory virtualThreads = CbThreads.virtualThreadFactoryOrNull("test-virtual-");
    assumeTrue(virtualThreads != null, "needs virtual threads");

    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread t = virtualThreads.newThread(() -> {
      try (InterruptWatcher.Registration ignored = watcher.watchCurrentThread(request)) {
        Thread.currentThread().interrupt();
        Thread.sleep(100);
      } catch (InterruptedException expected) {
        // from the sleep
      } catch (Throwable e) {
        failure.set(e);
      }
    });
    t.start();
    t.join();

    assertNull(failure.get());
    assertNull(watcher.checker(), "shouldn't start a thread for a virtual thread");
    verify(request, never()).cancel(any(CancellationReason.class));
  }

  @Test
  void closingARegistrationAgainHasNoEffect() throws Exception {
    // If it counted the thread out twice, the watcher would think there was nothing to watch, and park.
    InterruptWatcher.Registration registration = watcher.watchCurrentThread(request);
    registration.close();
    registration.close();
    waitUntilCondition(watcher::parked);

    startWatchedThread(false).interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
  }

  @Test
  void recoversIfTheCheckingThreadDies() throws Exception {
    Request<?> fatal = mock(Request.class);
    doThrow(new AssertionError("fatal")).when(fatal).cancel(CancellationReason.STOPPED_LISTENING);

    Thread watched = startWatchedThread(watcher, fatal, false);
    Thread first = watcher.checker();
    assertNotNull(first);

    watched.interrupt(); // cancelling throws, which ends the checking thread
    first.join(5000);
    assertFalse(first.isAlive());
    assertNull(watcher.checker(), "shouldn't keep pointing at a thread that's no longer checking");

    // The next registration starts a new one.
    startWatchedThread(false).interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
    assertNotSame(first, watcher.checker());
  }

  @Test
  void triesAgainIfTheCheckingThreadFailsToStart() throws Exception {
    AtomicBoolean failNextStart = new AtomicBoolean(true);
    InterruptWatcher unlucky = new InterruptWatcher(
      Duration.ofMillis(10),
      Duration.ofMillis(50),
      Duration.ofMinutes(1),
      "test-unlucky-interrupt-watcher",
      task -> {
        Thread t = new Thread(task, "test-unlucky-interrupt-watcher") {
          @Override
          public synchronized void start() {
            if (failNextStart.getAndSet(false)) {
              throw new OutOfMemoryError("unable to create native thread (simulated)");
            }
            super.start();
          }
        };
        t.setDaemon(true);
        return t;
      }
    );

    unlucky.watchCurrentThread(request).close(); // the call goes ahead unwatched
    assertNull(unlucky.checker(), "shouldn't mistake a thread that failed to start for one that's checking");

    startWatchedThread(unlucky, request, false).interrupt();
    verify(request, timeout(5000)).cancel(CancellationReason.STOPPED_LISTENING);
    assertNotNull(unlucky.checker());
  }
}
