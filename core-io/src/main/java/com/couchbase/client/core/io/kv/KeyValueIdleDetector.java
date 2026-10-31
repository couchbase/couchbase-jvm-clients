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

package com.couchbase.client.core.io.kv;

import org.jspecify.annotations.NullMarked;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Decides when a {@link NioKeyValueConnection} is dead, because the server has gone quiet.
 * <p>
 * If the server has been quiet for a third of the idle timeout (io.configIdleRedialTimeout), the connection sends
 * a NOOP, so a healthy server answers. If nothing arrives within another third, it sends a second NOOP; if nothing
 * arrives within a third after that one either, the connection closes. (So a dead connection closes when the server
 * has been quiet for the whole idle timeout.)
 * <p>
 * Waiting for two NOOPs means one long pause (a GC pause, for example) can't close a healthy connection: the check
 * might run before the reader has read a response that arrived during the pause, but by the next check it has.
 * <p>
 * The NOOPs are sent whether or not other requests are outstanding: with unordered execution (on by default),
 * the server answers a NOOP even while it works on a slow request. And a NOOP gives an idle connection unacknowledged
 * data, which is what the TCP user timeout applies to (on Linux, where it's set).
 * <p>
 * An idle timeout that's not positive turns this off.
 * The connection calls {@link #check} periodically.
 */
@NullMarked
final class KeyValueIdleDetector {

  /**
   * What the detector needs the connection to do.
   */
  interface Connection {
    /**
     * Sends a NOOP.
     */
    void sendNoop();

    /**
     * Closes the connection, since the server has gone quiet.
     *
     * @param reason why, for the log and the close cause.
     */
    void closeBecauseIdle(String reason);
  }

  /**
   * How many NOOPs in a row can go unanswered (each for a whole NOOP interval) before the connection closes.
   */
  static final int MAX_UNANSWERED_NOOPS = 2;

  /**
   * How often to {@link #check}, at most and at least. (Scheduling needs a positive interval.)
   */
  private static final long MAX_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static final long MIN_CHECK_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

  private final Duration idleTimeout;
  private final long noopIntervalNanos; // 0 if off
  private final long checkIntervalNanos;
  private final Connection connection;
  private final LongSupplier nanoTime;

  private volatile long lastReadNanos;

  // Only the checking thread uses these.
  private int unansweredNoops; // sent since the server last sent something
  private long lastNoopNanos;

  KeyValueIdleDetector(Duration configIdleRedialTimeout, Connection connection) {
    this(configIdleRedialTimeout, connection, System::nanoTime);
  }

  /**
   * @param nanoTime the clock, like {@link System#nanoTime()}.
   */
  KeyValueIdleDetector(Duration configIdleRedialTimeout, Connection connection, LongSupplier nanoTime) {
    this.idleTimeout = requireNonNull(configIdleRedialTimeout);
    long timeoutNanos = configIdleRedialTimeout.toNanos();
    this.noopIntervalNanos = timeoutNanos > 0 ? Math.max(1, timeoutNanos / (MAX_UNANSWERED_NOOPS + 1)) : 0;
    this.checkIntervalNanos = noopIntervalNanos > 0
      ? Math.max(MIN_CHECK_INTERVAL_NANOS, Math.min(noopIntervalNanos, MAX_CHECK_INTERVAL_NANOS))
      : MAX_CHECK_INTERVAL_NANOS;
    this.connection = requireNonNull(connection);
    this.nanoTime = requireNonNull(nanoTime);
  }

  /**
   * How often to call {@link #check}: at most a second apart (more often for a short idle timeout).
   */
  long checkIntervalNanos() {
    return checkIntervalNanos;
  }

  /**
   * How long the server can be quiet before the connection sends a NOOP (a third of the idle timeout), or 0 if it never does.
   */
  long noopIntervalNanos() {
    return noopIntervalNanos;
  }

  /**
   * Starts timing, as if the server just sent something. Call once, when the connection starts.
   */
  void start() {
    lastReadNanos = nanoTime.getAsLong();
  }

  /**
   * Call when the connection reads something.
   */
  void read() {
    lastReadNanos = nanoTime.getAsLong();
  }

  /**
   * Call at least every {@link #checkIntervalNanos}, from one thread at a time: sends a NOOP, or closes the
   * connection, if the server has been quiet long enough (see the class description).
   */
  void check() {
    if (noopIntervalNanos == 0) {
      return;
    }
    long now = nanoTime.getAsLong();
    long lastRead = lastReadNanos;
    long quietNanos = now - lastRead;
    if (quietNanos < noopIntervalNanos) {
      return;
    }

    if (unansweredNoops > 0 && lastRead - lastNoopNanos > 0) {
      unansweredNoops = 0; // the server has sent something since
    }
    if (unansweredNoops > 0 && now - lastNoopNanos < noopIntervalNanos) {
      return; // still waiting for an answer
    }
    if (unansweredNoops >= MAX_UNANSWERED_NOOPS) {
      connection.closeBecauseIdle(
        "No incoming traffic for " + Duration.ofNanos(quietNanos) + ", including a third of the idle timeout"
          + " (io.configIdleRedialTimeout = " + idleTimeout + ") after each of the last " + unansweredNoops + " NOOPs"
      );
      return;
    }
    unansweredNoops++;
    lastNoopNanos = now;
    connection.sendNoop();
  }
}
