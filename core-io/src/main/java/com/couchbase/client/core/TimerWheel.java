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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * Runs tasks after a delay, on one thread, using a hashed timing wheel: the same algorithm as Netty's
 * {@code HashedWheelTimer}, which this replaces.
 * <p>
 * Scheduling and cancelling a task take constant time and no locks, which matters because every request
 * schedules its timeout, and almost always cancels it. A task runs about a tick after its delay elapses at most,
 * and never before.
 *
 * <h2>The wheel</h2>
 * Time is measured in nanoseconds from when the wheel was created, and divided into ticks of {@code tickNanos}:
 * tick {@code k} covers {@code [k * tickNanos, (k + 1) * tickNanos)}. The wheel is a circular array of buckets
 * (a power of two of them, so {@code tick & mask} picks one), and each bucket is a doubly linked list of entries,
 * so an entry can be removed in constant time. At the end of each tick, the thread runs the tasks due in that
 * tick's bucket.
 * <pre>
 *  tick:     0     1     2          511   512 (bucket 0 again)
 *          +-----+-----+-----+ ... +-----+
 *  bucket: |  0  |  1  |  2  |     | 511 |
 *          +-----+-----+-----+ ... +-----+
 * </pre>
 * A task due at {@code deadline} belongs to tick {@code dueTick = deadline / tickNanos}, and so to that tick's
 * bucket. One turn of the wheel takes {@code buckets * tickNanos} (5.12 seconds with {@link Timer}'s default
 * 10 ms tick and 512 buckets), so a task due further out shares its bucket with tasks due one or more turns
 * sooner. Each entry counts the turns left before it's due ({@code remainingRounds}).
 *
 * <h2>Threads</h2>
 * Only the wheel's thread touches the buckets, and the fields of entries that place them in buckets. Other threads
 * hand it work through two queues ({@link LinkedQueue}): {@code pending} for new tasks, and {@code cancelled}
 * for cancelled ones.
 * <ul>
 * <li>{@link #newTimeout} starts the thread if needed, computes the deadline, and adds a new entry to
 * {@code pending}. The entry is the only allocation.
 * <li>{@link Entry#cancel()} changes the entry's state from {@code INIT} to {@code CANCELLED}, drops its task,
 * and adds the entry to {@code cancelled}, so the thread removes it from its bucket at the next tick, instead of
 * keeping it until it would have run.
 * <li>Running a task changes the entry's state from {@code INIT} to {@code EXPIRED}, so exactly one of running
 * and cancelling wins.
 * </ul>
 *
 * <h2>Each tick</h2>
 * The thread:
 * <ol>
 * <li>Waits until the tick ends, at {@code (tick + 1) * tickNanos}. Measuring from the start, rather than from
 * the last wakeup, keeps delays from adding up. If the thread is behind (after a slow task or a GC pause),
 * the tick has already ended, so it catches up one tick at a time, without waiting.
 * <li>Removes the cancelled entries from their buckets.
 * <li>Moves up to {@code MAX_TRANSFERS_PER_TICK} new entries into their buckets, skipping cancelled ones.
 * An entry gets {@code remainingRounds = (dueTick - tick) / buckets}, and goes in the bucket of
 * {@code max(dueTick, tick)}, so one that's already due goes in the current bucket.
 * <li>Goes through the current bucket: it runs the entries with no rounds left, removes cancelled entries, and
 * counts down the rounds of the others.
 * </ol>
 * For example, with a 10 ms tick and 512 buckets, at tick 100:
 * <ul>
 * <li>A 2.5 second timeout is due at tick 350. It goes in bucket 350 with 0 rounds, and runs at the end of
 * tick 350.
 * <li>A 7 second timeout is due at tick 800. It goes in bucket {@code 800 & 511 = 288} with 1 round. At tick 288,
 * its round is counted down, and at tick 800, it runs.
 * </ul>
 * A task never runs early: a tick's bucket is processed after the tick ends, so after the deadlines of its tasks.
 * A task runs at the end of the tick its deadline is in, so usually less than a tick late, plus however long
 * the OS takes to wake the thread, and any tasks that run before it. Tasks run one at a time, so they must be
 * quick. A task may schedule or cancel tasks (which changes only the queues, not the buckets), but not stop the
 * timer.
 *
 * <h2>Stopping</h2>
 * {@link #stop()} changes the state, wakes the thread, and waits for it to exit. Before exiting, the thread
 * collects the tasks that didn't run and weren't cancelled, from the buckets and from {@code pending}, for
 * {@code stop()} to return. A task scheduled just as the timer stops might be missed, so {@link Timer} checks for
 * that itself.
 *
 * <h2>Costs</h2>
 * Scheduling a task allocates its entry (about 64 bytes), and cancelling allocates nothing. Each tick takes time
 * in proportion to the tasks scheduled and cancelled since the last tick, plus the tasks in the current bucket.
 * A task due several turns away is visited every turn, to count down its rounds, so a turn of the wheel should be
 * longer than most delays.
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

  private final LinkedQueue<Entry> pending = new LinkedQueue<>(Entry.NEXT_PENDING, new Entry(this, () -> { }, 0));
  private final LinkedQueue<Entry> cancelled = new LinkedQueue<>(Entry.NEXT_CANCELLED, new Entry(this, () -> { }, 0));

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

    long delayNanos = unit.toNanos(delay); // saturates at Long.MAX_VALUE
    long elapsed = System.nanoTime() - startTime; // the wheel's age, never negative
    // A delay this long (about 292 years, like a "no timeout" sentinel) would overflow, and the task would look
    // overdue and run at once. Treat it as never due instead.
    long deadline = delayNanos > Long.MAX_VALUE - elapsed ? Long.MAX_VALUE : elapsed + delayNanos;

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
    // Wait out adds in progress: a task behind one might have been scheduled before stop(), and must be returned.
    Entry entry;
    while ((entry = pending.pollWaitingForAdds()) != null) {
      if (!entry.isCancelled()) {
        unprocessed.add(entry);
      }
    }
    while (cancelled.poll() != null) {
      // discard
    }
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
      // parkNanos returns at once while the thread is interrupted, so a leftover interrupt would make this spin.
      // Interrupts mean nothing here: stop() changes the state and unparks.
      Thread.interrupted();
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
    private static final AtomicReferenceFieldUpdater<Entry, Entry> NEXT_PENDING =
        AtomicReferenceFieldUpdater.newUpdater(Entry.class, Entry.class, "nextPending");
    private static final AtomicReferenceFieldUpdater<Entry, Entry> NEXT_CANCELLED =
        AtomicReferenceFieldUpdater.newUpdater(Entry.class, Entry.class, "nextCancelled");

    private final TimerWheel timer;

    /**
     * Null once the task runs, or is cancelled, so it (and whatever it references, like a request) can be
     * garbage collected while this entry is still reachable: a queue keeps the last entry it returned, and the
     * caller may keep the registration.
     */
    private @Nullable Runnable task;

    /**
     * When the task is due, relative to the wheel's start time.
     */
    private final long deadline;

    private volatile int state = ST_INIT;

    // The links of the pending and cancelled queues. Two, because a task cancelled before the thread moves it
    // into the wheel is in both. They look unused because only NEXT_PENDING and NEXT_CANCELLED access them,
    // by name.
    @SuppressWarnings("unused") private volatile @Nullable Entry nextPending;
    @SuppressWarnings("unused") private volatile @Nullable Entry nextCancelled;

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
     * Returns the task this runs, or null if it ran or was cancelled.
     */
    @Nullable Runnable task() {
      return task;
    }

    @Override
    public boolean cancel() {
      if (!STATE.compareAndSet(this, ST_INIT, ST_CANCELLED)) {
        return false;
      }
      task = null; // won't run now
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
      Runnable toRun = requireNonNull(task); // only cancel() clears it, after winning the state change
      task = null;
      try {
        toRun.run();
      } catch (Throwable t) {
        log.warn("A timer task threw an exception.", t);
      }
      Thread.interrupted(); // a task that leaves the thread interrupted mustn't affect the next one
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
   * A queue that any thread can add to, and one thread polls. Its items are its nodes, linked through a field of
   * their own, so adding one allocates nothing. Each item can be added only once, ever.
   * <p>
   * This is Dmitry Vyukov's multiple-producer, single-consumer linked queue.
   */
  static final class LinkedQueue<T> {
    // Not static: each queue links its items through a different field (an entry can be in both the pending and
    // cancelled queues at once). A static updater would be a little faster, since the JIT could treat it
    // as a constant, but that would take a separate queue class per field.
    @SuppressWarnings("AtomicFieldUpdaterNotStaticFinal") private final AtomicReferenceFieldUpdater<T, @Nullable T> link;
    private final AtomicReference<T> tail;

    /**
     * The last item polled (at first, a placeholder that's never returned). Used only by the polling thread.
     */
    private T head;

    /**
     * @param link updates the item's field that links it to the next item
     * @param placeholder an item that's never added, to start the queue
     */
    LinkedQueue(AtomicReferenceFieldUpdater<T, @Nullable T> link, T placeholder) {
      this.link = requireNonNull(link);
      this.head = requireNonNull(placeholder);
      this.tail = new AtomicReference<>(placeholder);
    }

    void add(T item) {
      T previous = tail.getAndSet(item);
      // Until this, the item is the tail but not yet reachable from the head, so poll() doesn't see it yet.
      link.lazySet(previous, item);
    }

    /**
     * Returns the next item, or null if the queue is empty (or the next item's add() hasn't finished).
     * Must be called only by the polling thread.
     */
    @Nullable T poll() {
      T next = link.get(head);
      if (next == null) {
        return null;
      }
      link.lazySet(head, null); // so an item that's still in use doesn't keep later items reachable
      head = next;
      return next;
    }

    /**
     * Like {@link #poll()}, but if an add() is in progress, waits for it to finish instead of returning null.
     * (Items added after that one, even by add() calls that returned, aren't reachable until it does.)
     * So this returns null only if the queue is empty. Must be called only by the polling thread.
     */
    @Nullable T pollWaitingForAdds() {
      while (true) {
        T next = poll();
        if (next != null || head == tail.get()) {
          return next;
        }
        Thread.yield();
      }
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
