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

import com.couchbase.client.core.util.CbThreads;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Decides when a {@link KeyValueConnection} has been idle too long, like the Netty implementation's
 * IdleStateHandler (with KeyValueMessageHandler): if the server has been quiet for half the idle timeout
 * (io.configIdleRedialTimeout), the connection sends a NOOP, so a healthy server answers; if it's quiet for
 * all of it, the connection closes.
 * <p>
 * The writer thread checks (see {@link #check}), since it might send the NOOP. If the writer is stuck writing
 * (because the server isn't reading), a scheduled task checks instead (see {@link #checkStuckWriter}).
 * <p>
 * An idle timeout that's not positive turns idle detection off, like the Netty implementation.
 */
@NullMarked
final class KeyValueIdleDetector {

  /**
   * What the detector needs the connection to do.
   */
  interface Connection {
    /**
     * Writes a NOOP. Called by the writer thread, from {@link #check}.
     */
    void writeNoop() throws IOException;

    /**
     * Closes the connection, since the server has been quiet for the whole idle timeout.
     */
    void closeBecauseIdle();
  }

  /**
   * Runs {@link #checkStuckWriter} for every connection. One thread is plenty: the checks are quick.
   */
  private static final ScheduledThreadPoolExecutor stuckWriterChecks = newStuckWriterChecks();

  private static ScheduledThreadPoolExecutor newStuckWriterChecks() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, CbThreads.platformThreadFactory("cb-kv-idle-"));
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  /**
   * How often the writer checks whether the server has been quiet, at most.
   */
  private static final long MAX_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

  /**
   * Like the Netty implementation's IdleStateHandler, which waits at least this long.
   */
  private static final long MIN_READER_IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

  /**
   * Returns how long the server can be quiet before the connection sends a NOOP: half the idle timeout.
   * Or 0 if there's no idle detection: like the Netty implementation's IdleStateHandler, if it's not positive.
   */
  static long readerIdleNanos(Duration configIdleRedialTimeout) {
    long half = configIdleRedialTimeout.toNanos() / 2;
    return half <= 0 ? 0 : Math.max(half, MIN_READER_IDLE_NANOS);
  }

  private final long readerIdleNanos;
  private final long checkIntervalNanos;
  private final Connection connection;
  private final LongSupplier nanoTime;

  private volatile long lastReadNanos;
  private volatile boolean noopSentSinceLastRead;

  // Only accessed by the writer.
  private long noopSentNanos;

  /**
   * Whether the writer is writing (so it might be blocked, and can't run {@link #check}).
   */
  private volatile boolean writing;

  // Guarded by "this".
  private @Nullable ScheduledFuture<?> stuckWriterCheck;
  private boolean stopped;

  KeyValueIdleDetector(Duration configIdleRedialTimeout, Connection connection) {
    this(configIdleRedialTimeout, connection, System::nanoTime);
  }

  /**
   * @param nanoTime the clock, like {@link System#nanoTime()}.
   */
  KeyValueIdleDetector(Duration configIdleRedialTimeout, Connection connection, LongSupplier nanoTime) {
    this.readerIdleNanos = readerIdleNanos(configIdleRedialTimeout);
    this.checkIntervalNanos = Math.min(readerIdleNanos, MAX_CHECK_INTERVAL_NANOS);
    this.connection = requireNonNull(connection);
    this.nanoTime = requireNonNull(nanoTime);
  }

  /**
   * How long the writer can wait for requests before it should {@link #check} again, or 0 if it can wait as long as it
   * takes (when idle detection is off).
   */
  long checkIntervalNanos() {
    return checkIntervalNanos;
  }

  /**
   * Starts timing (as if the server just sent something), and the stuck-writer checks. Call once,
   * when the connection starts.
   */
  void start() {
    lastReadNanos = nanoTime.getAsLong();
    if (enabled()) {
      synchronized (this) {
        // Not once stopped: nothing would cancel it.
        if (!stopped) {
          stuckWriterCheck = stuckWriterChecks.scheduleWithFixedDelay(
            this::checkStuckWriter, checkIntervalNanos, checkIntervalNanos, TimeUnit.NANOSECONDS
          );
        }
      }
    }
  }

  /**
   * Stops the stuck-writer checks. Call when the connection closes (even if it never started).
   */
  synchronized void stop() {
    stopped = true;
    if (stuckWriterCheck != null) {
      stuckWriterCheck.cancel(false);
      stuckWriterCheck = null;
    }
  }

  /**
   * Call from the reader thread when it reads something.
   */
  void read() {
    lastReadNanos = nanoTime.getAsLong();
    noopSentSinceLastRead = false;
  }

  /**
   * Call from the writer thread before it writes (and so might be blocked).
   */
  void startedWriting() {
    writing = true;
  }

  /**
   * Call from the writer thread when it's done writing (after {@link #check}).
   */
  void finishedWriting() {
    writing = false;
  }

  /**
   * Call from the writer thread, at least every {@link #checkIntervalNanos}: if the server has been quiet for half
   * the idle timeout, writes a NOOP; if it's quiet for all of it, closes the connection.
   */
  void check() throws IOException {
    if (!enabled()) {
      return;
    }
    long now = nanoTime.getAsLong();
    if (!noopSentSinceLastRead) {
      if (now - lastReadNanos >= readerIdleNanos) {
        noopSentNanos = now;
        noopSentSinceLastRead = true;
        connection.writeNoop();
      }

    } else if (now - noopSentNanos >= readerIdleNanos) {
      // Nothing for a whole idle period after the NOOP (measured from when it was sent, like the Netty
      // implementation's second idle event), so nothing for at least io.configIdleRedialTimeout in all.
      connection.closeBecauseIdle();
    }
  }

  /**
   * Does what {@link #check} would, if the writer is stuck writing (because the server isn't reading):
   * closes the connection if the server has been quiet for io.configIdleRedialTimeout. Like the Netty
   * implementation, whose idle check runs whether or not its writes are stuck.
   */
  void checkStuckWriter() {
    if (enabled() && writing && nanoTime.getAsLong() - lastReadNanos >= 2 * readerIdleNanos) {
      connection.closeBecauseIdle();
    }
  }

  private boolean enabled() {
    return readerIdleNanos > 0;
  }
}
