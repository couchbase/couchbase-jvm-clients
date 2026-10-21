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

import com.couchbase.client.core.msg.BlockingCaller;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.util.CbThreads;
import okhttp3.Call;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * Cancels a request if the platform thread sending it on its own behalf (its {@link BlockingCaller})
 * is interrupted while it waits for the response.
 * <p>
 * Interrupting a platform thread doesn't interrupt a socket read (unlike a virtual thread, whose socket is closed),
 * so without this, the thread wouldn't notice it was interrupted until the response arrived or the request timed out.
 * Instead, this checks the threads it watches every so often, and cancels the request of one that was interrupted.
 * That cancels the HTTP call, which closes the connection, so the read fails, and the thread
 * finds out it was interrupted (see {@link BlockingCaller}). It also cancels the call itself, if it was given one:
 * a streamed result's request completes when the header arrives, so cancelling the request has no effect
 * while the rest of the result is being read.
 * <p>
 * One thread does the checking for every call. It's started when first needed, and costs little when there's
 * nothing to watch:
 * <ul>
 *   <li>It keeps checking for a while after the last call finishes, so a thread sending one request after another
 *       doesn't have to wake it for each one.
 *   <li>Then it parks, until the next call wakes it.
 *   <li>After a longer idle period, it exits (the next call starts a new one), so it doesn't outlive
 *       an application that's done with the SDK (for example, one that was undeployed).
 * </ul>
 * Cancelling a request completes it, so whatever runs when a request completes (ending its span, recording
 * metrics, converting the error, waking the caller) runs on the checking thread, like it runs on the timer's thread
 * when a request times out. If that's slow (for example, a custom tracer), checking the other threads waits for it.
 */
@NullMarked
final class InterruptWatcher {
  private static final Logger log = LoggerFactory.getLogger(InterruptWatcher.class);

  static final InterruptWatcher INSTANCE = new InterruptWatcher(
    Duration.ofMillis(50),
    Duration.ofSeconds(1),
    Duration.ofSeconds(60),
    "cb-interrupt-watcher"
  );

  /**
   * Stops watching a thread. Closing it again has no effect.
   */
  @FunctionalInterface
  interface Registration extends AutoCloseable {
    @Override
    void close();
  }

  private static final Registration NOT_WATCHED = () -> {
  };

  private final class Watched implements Registration {
    final Thread thread;
    final Request<?> request;
    final @Nullable Call call;

    /**
     * Only the watched thread closes its registration, so this doesn't need to be volatile.
     */
    private boolean closed;

    Watched(Thread thread, Request<?> request, @Nullable Call call) {
      this.thread = thread;
      this.request = request;
      this.call = call;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      watched.remove(this);
      count.decrementAndGet();
    }
  }

  private final long intervalNanos;
  private final long parkAfterNanos;
  private final long exitAfterNanos;
  private final String threadName;
  private final ThreadFactory threadFactory;
  private final Set<Watched> watched = ConcurrentHashMap.newKeySet();

  /**
   * The number of threads being watched.
   */
  private final AtomicInteger count = new AtomicInteger();

  /**
   * The checking thread, or null if there isn't one. Changed only while holding this object's lock.
   */
  private volatile @Nullable Thread checker;

  /**
   * True while the checking thread is parked (or about to park), and must be woken to check a new registration.
   * <p>
   * The checking thread sets this, then checks whether there's anything to watch, before parking;
   * a registration counts itself, then checks this. So at least one of them sees the other's change:
   * the checking thread doesn't park, or the registration wakes it.
   */
  private volatile boolean parked;

  /**
   * The number of times a registration woke (or started) the checking thread. For tests.
   */
  private final AtomicInteger wakeUps = new AtomicInteger();

  /**
   * @param interval how often to check the watched threads
   * @param parkAfter how long to keep checking after the last thread stops being watched, before parking
   * @param exitAfter how long to wait after the last thread stops being watched, before the checking thread exits
   */
  InterruptWatcher(Duration interval, Duration parkAfter, Duration exitAfter, String threadName) {
    this(interval, parkAfter, exitAfter, threadName, task -> {
      Thread t = new Thread(task, threadName);
      t.setDaemon(true);
      t.setContextClassLoader(null); // don't keep the creating code's class loader reachable
      return t;
    });
  }

  /**
   * @param threadFactory creates the checking thread. For tests.
   */
  InterruptWatcher(Duration interval, Duration parkAfter, Duration exitAfter, String threadName, ThreadFactory threadFactory) {
    this.intervalNanos = interval.toNanos();
    this.parkAfterNanos = parkAfter.toNanos();
    this.exitAfterNanos = exitAfter.toNanos();
    this.threadName = requireNonNull(threadName);
    this.threadFactory = requireNonNull(threadFactory);
  }

  /**
   * Watches the current thread, if it's a platform thread, until the returned registration is closed:
   * if the thread is interrupted, cancels the request ({@link CancellationReason#STOPPED_LISTENING}).
   */
  Registration watchCurrentThread(Request<?> request) {
    return watchCurrentThread(request, null);
  }

  /**
   * Like {@link #watchCurrentThread(Request)}, but also cancels the call (after the request).
   */
  Registration watchCurrentThread(Request<?> request, @Nullable Call call) {
    Thread current = Thread.currentThread();
    if (CbThreads.isVirtual(current)) {
      return NOT_WATCHED;
    }

    Watched w = new Watched(current, request, call);
    watched.add(w);
    count.incrementAndGet();
    if (parked || checker == null) {
      wakeChecker();
    }
    return w;
  }

  private synchronized void wakeChecker() {
    Thread t = checker;
    if (t == null) {
      wakeUps.incrementAndGet();
      parked = false;
      t = threadFactory.newThread(this::check);
      try {
        t.start();
      } catch (OutOfMemoryError | RuntimeException e) {
        // For example, the JVM can't create another thread. The call goes ahead unwatched;
        // the next one tries again, since there's still no checking thread.
        log.warn("Failed to start {}; an interrupted platform thread waiting for a response won't notice until it arrives.", threadName, e);
        return;
      }
      // Only once it started, so a failure to start isn't mistaken for a thread that's checking.
      // (It's safe to publish afterwards: the new thread reads this field only while holding this object's lock.)
      checker = t;

    } else if (parked) {
      wakeUps.incrementAndGet();
      parked = false;
      LockSupport.unpark(t); // if it hasn't parked yet, the permit makes it return right away
    }
  }

  private void check() {
    try {
      checkUntilIdle();
    } finally {
      // If it ended with an exception (it isn't expected to), let the next registration start a new one,
      // instead of leaving the field pointing at a thread that's no longer checking.
      synchronized (this) {
        if (checker == Thread.currentThread()) {
          checker = null;
          parked = false;
        }
      }
    }
  }

  private void checkUntilIdle() {
    long idleSince = 0; // System.nanoTime() when it last found nothing to watch, or 0 if it's watching something

    while (true) {
      if (count.get() > 0) {
        idleSince = 0;
      } else {
        long now = System.nanoTime();
        if (idleSince == 0) {
          idleSince = now;
        }
        long idle = now - idleSince;

        if (idle >= exitAfterNanos) {
          synchronized (this) {
            checker = null;
            // Checked after clearing the field, so a registration that counted itself before this sees the
            // field cleared and starts a new thread once this one releases the lock, or this one sees it and stays.
            if (count.get() == 0) {
              return;
            }
            checker = Thread.currentThread();
          }
          continue;
        }

        if (idle >= parkAfterNanos) {
          parked = true;
          if (count.get() == 0) {
            LockSupport.parkNanos(this, exitAfterNanos - idle);
          }
          parked = false;
          continue;
        }
      }

      LockSupport.parkNanos(this, intervalNanos);

      for (Watched w : watched) {
        if (w.thread.isInterrupted()) {
          watched.remove(w); // cancel once; the registration still counts until it's closed
          try {
            w.request.cancel(CancellationReason.STOPPED_LISTENING);
          } catch (RuntimeException e) {
            log.warn("Failed to cancel {} after its thread was interrupted", w.request.getClass().getSimpleName(), e);
          }
          if (w.call != null) {
            w.call.cancel();
          }
        }
      }
    }
  }

  /**
   * Returns the checking thread, or null if there isn't one. For tests.
   */
  @Nullable Thread checker() {
    return checker;
  }

  /**
   * Returns true if the checking thread is parked. For tests.
   */
  boolean parked() {
    return parked;
  }

  /**
   * Returns the number of times a registration woke (or started) the checking thread. For tests.
   */
  int wakeUps() {
    return wakeUps.get();
  }
}
