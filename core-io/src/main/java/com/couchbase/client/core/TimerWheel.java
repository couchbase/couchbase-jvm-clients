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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * Runs tasks after a delay, on one thread, using a hashed wheel: time is divided into ticks, and each tick
 * has a bucket of the tasks due then (a bucket holds the tasks of every tick that maps to it, one "round"
 * of the wheel apart).
 * <p>
 * Scheduling and cancelling a task take constant time and no locks, which matters because every request
 * schedules its timeout, and almost always cancels it. A task runs within a tick after its delay elapses,
 * never before. A cancelled task is dropped from its bucket at the next tick, so it isn't kept until it would
 * have run.
 * <p>
 * New tasks wait in a queue until the thread moves them into their buckets, and cancelled ones in another queue
 * until it removes them, so only the thread touches the buckets.
 * <p>
 * Uses the same algorithm as Netty's {@code HashedWheelTimer}, which this replaces.
 */
@NullMarked
final class TimerWheel {
  private static final Logger log = LoggerFactory.getLogger(TimerWheel.class);

  /**
   * At most this many new tasks are moved into the wheel per tick, so a burst of new tasks can't delay the tasks
   * due at that tick for long. The rest are moved at the next ticks.
   */
  private static final int MAX_TRANSFERS_PER_TICK = 100_000;

  private static final long MIN_TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
  private static final int MAX_BUCKETS = 1 << 30;

  private static final int INIT = 0;
  private static final int STARTED = 1;
  private static final int STOPPED = 2;

  private final AtomicInteger state = new AtomicInteger(INIT);
  private final Thread worker;

  /**
   * The origin of the wheel's time: deadlines and tick times are measured from here.
   */
  private final long startTime = System.nanoTime();
  private final long tickNanos;
  private final Bucket[] wheel;
  private final int mask;

  private final Queue<Entry> pending = new ConcurrentLinkedQueue<>();
  private final Queue<Entry> cancelled = new ConcurrentLinkedQueue<>();

  // Used only by the worker, until it exits; then read by stop(), after joining it.
  private long tick;
  private final Set<Entry> unprocessed = new HashSet<>();

  /**
   * @param tickDuration how often the tasks due are run (at least 1 millisecond)
   * @param numBuckets how many buckets the wheel has (rounded up to a power of two)
   * @param threadName the name of the thread that runs the tasks (a daemon thread)
   */
  TimerWheel(Duration tickDuration, int numBuckets, String threadName) {
    if (tickDuration.isNegative() || tickDuration.isZero()) {
      throw new IllegalArgumentException("tickDuration must be positive, but was " + tickDuration);
    }
    if (numBuckets <= 0 || numBuckets > MAX_BUCKETS) {
      throw new IllegalArgumentException("numBuckets must be between 1 and " + MAX_BUCKETS + ", but was " + numBuckets);
    }

    long nanos = tickDuration.toNanos();
    if (nanos < MIN_TICK_NANOS) {
      log.warn("Timer tick duration {} is less than 1 millisecond; using 1 millisecond.", tickDuration);
      nanos = MIN_TICK_NANOS;
    }
    this.tickNanos = nanos;

    int size = Integer.highestOneBit(numBuckets);
    if (size < numBuckets) {
      size <<= 1;
    }
    this.wheel = new Bucket[size];
    for (int i = 0; i < size; i++) {
      wheel[i] = new Bucket();
    }
    this.mask = size - 1;

    this.worker = new Thread(this::run, requireNonNull(threadName));
    worker.setDaemon(true);
  }

  /**
   * Starts the thread, unless it's already started.
   *
   * @throws IllegalStateException if this was stopped
   */
  void start() {
    int s = state.get();
    if (s == INIT && state.compareAndSet(INIT, STARTED)) {
      worker.start();
    } else if (s == STOPPED || state.get() == STOPPED) {
      throw new IllegalStateException("Can't start a timer once it's stopped");
    }
  }

  /**
   * Schedules the task to run after the delay (on this timer's thread), starting the timer if needed.
   * <p>
   * A task scheduled as the timer stops might never run, and not be returned by {@link #stop}.
   *
   * @throws IllegalStateException if this was stopped
   */
  Timer.Registration newTimeout(Runnable task, long delay, TimeUnit unit) {
    requireNonNull(task);
    start();

    long delayNanos = unit.toNanos(delay);
    long deadline = System.nanoTime() + delayNanos - startTime;
    if (delayNanos > 0 && deadline < 0) {
      deadline = Long.MAX_VALUE; // overflowed
    }

    Entry entry = new Entry(this, task, deadline);
    pending.add(entry);
    return entry;
  }

  /**
   * Stops the timer: its thread exits, without running any more tasks.
   * Must not be called on the timer's thread.
   *
   * @return the tasks that were scheduled, and not cancelled, but didn't run
   */
  Set<Entry> stop() {
    if (Thread.currentThread() == worker) {
      throw new IllegalStateException("Can't stop a timer from one of its tasks");
    }

    int previous = state.getAndSet(STOPPED);
    if (previous != STARTED) {
      return Collections.emptySet(); // never started (so nothing was scheduled), or already stopped
    }

    boolean interrupted = false;
    while (worker.isAlive()) {
      LockSupport.unpark(worker);
      try {
        worker.join(100);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return Collections.unmodifiableSet(unprocessed);
  }

  private void run() {
    while (true) {
      long now = waitForNextTick();
      if (now < 0) {
        break; // stopped
      }
      removeCancelled();
      transferPending();
      wheel[(int) (tick & mask)].expire(now);
      tick++;
    }

    // Stopped. Keep what didn't run for stop() to return.
    for (Bucket bucket : wheel) {
      bucket.drainInto(unprocessed);
    }
    Entry entry;
    while ((entry = pending.poll()) != null) {
      if (!entry.isCancelled()) {
        unprocessed.add(entry);
      }
    }
    cancelled.clear();
  }

  /**
   * Waits until the current tick ends.
   *
   * @return the time (relative to the start time), or -1 if the timer was stopped
   */
  private long waitForNextTick() {
    long tickEnd = tickNanos * (tick + 1);
    while (true) {
      if (state.get() != STARTED) {
        return -1;
      }
      long now = System.nanoTime() - startTime;
      long remaining = tickEnd - now;
      if (remaining <= 0) {
        return now;
      }
      LockSupport.parkNanos(this, remaining);
    }
  }

  private void transferPending() {
    for (int i = 0; i < MAX_TRANSFERS_PER_TICK; i++) {
      Entry entry = pending.poll();
      if (entry == null) {
        return;
      }
      if (entry.isCancelled()) {
        continue;
      }

      long dueTick = entry.deadline / tickNanos;
      entry.remainingRounds = (dueTick - tick) / wheel.length;
      // A task already due (scheduled in this tick, or the thread is behind) goes in the current tick's bucket.
      long ticks = Math.max(dueTick, tick);
      wheel[(int) (ticks & mask)].add(entry);
    }
  }

  private void removeCancelled() {
    Entry entry;
    while ((entry = cancelled.poll()) != null) {
      Bucket bucket = entry.bucket;
      if (bucket != null) {
        bucket.remove(entry);
      }
    }
  }

  /**
   * A scheduled task.
   */
  static final class Entry implements Timer.Registration {
    private static final int ST_INIT = 0;
    private static final int ST_CANCELLED = 1;
    private static final int ST_EXPIRED = 2;

    private static final AtomicIntegerFieldUpdater<Entry> STATE = AtomicIntegerFieldUpdater.newUpdater(Entry.class, "state");

    private final TimerWheel timer;
    private final Runnable task;

    /**
     * When the task is due, relative to the wheel's start time.
     */
    private final long deadline;

    private volatile int state = ST_INIT;

    // Used only by the timer's thread.
    private long remainingRounds;
    private @Nullable Entry next;
    private @Nullable Entry prev;
    private @Nullable Bucket bucket;

    private Entry(TimerWheel timer, Runnable task, long deadline) {
      this.timer = timer;
      this.task = task;
      this.deadline = deadline;
    }

    /**
     * Returns the task this runs.
     */
    Runnable task() {
      return task;
    }

    @Override
    public boolean cancel() {
      if (!STATE.compareAndSet(this, ST_INIT, ST_CANCELLED)) {
        return false;
      }
      if (timer.state.get() == STARTED) {
        timer.cancelled.add(this); // so the thread removes it from its bucket
      }
      return true;
    }

    boolean isCancelled() {
      return state == ST_CANCELLED;
    }

    private void expire() {
      if (!STATE.compareAndSet(this, ST_INIT, ST_EXPIRED)) {
        return;
      }
      try {
        task.run();
      } catch (Throwable t) {
        log.warn("A timer task threw an exception.", t);
      }
    }

    @Override
    public String toString() {
      return "TimerWheel.Entry{" +
          "task=" + task +
          ", state=" + state +
          '}';
    }
  }

  /**
   * The tasks of a bucket, as a doubly linked list. Used only by the timer's thread.
   */
  private static final class Bucket {
    private @Nullable Entry head;
    private @Nullable Entry tail;

    void add(Entry entry) {
      entry.bucket = this;
      if (head == null) {
        head = tail = entry;
      } else {
        requireNonNull(tail).next = entry;
        entry.prev = tail;
        tail = entry;
      }
    }

    /**
     * Runs the tasks due by now, and counts down the rounds of the others.
     */
    void expire(long now) {
      Entry entry = head;
      while (entry != null) {
        Entry next = entry.next;
        if (entry.remainingRounds <= 0) {
          remove(entry);
          if (entry.deadline <= now) {
            entry.expire();
          } else {
            // Not possible: a task is put in the bucket of the tick it's due in (or a later one), and a tick's bucket
            // is expired after the tick ends. But don't lose the task: run it now rather than never.
            log.warn("Timer task {} expired {} ns early", entry, entry.deadline - now);
            entry.expire();
          }
        } else if (entry.isCancelled()) {
          remove(entry);
        } else {
          entry.remainingRounds--;
        }
        entry = next;
      }
    }

    void remove(Entry entry) {
      if (entry.bucket != this) {
        return; // already removed
      }
      Entry next = entry.next;
      Entry prev = entry.prev;
      if (prev != null) {
        prev.next = next;
      } else {
        head = next;
      }
      if (next != null) {
        next.prev = prev;
      } else {
        tail = prev;
      }
      entry.next = null;
      entry.prev = null;
      entry.bucket = null;
    }

    void drainInto(Set<Entry> unprocessed) {
      Entry entry = head;
      while (entry != null) {
        Entry next = entry.next;
        remove(entry);
        if (entry.state == Entry.ST_INIT) {
          unprocessed.add(entry);
        }
        entry = next;
      }
    }
  }

  @Override
  public String toString() {
    return "TimerWheel{" +
        "thread=" + worker.getName() +
        ", tickNanos=" + tickNanos +
        ", buckets=" + wheel.length +
        ", state=" + state.get() +
        '}';
  }
}
