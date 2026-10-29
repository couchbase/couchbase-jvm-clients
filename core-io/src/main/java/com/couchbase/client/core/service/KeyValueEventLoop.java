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

import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread that reads and writes many {@link NioKeyValueConnection}s, with one {@link Selector}.
 * <p>
 * Each pass of the loop:
 * <ol>
 *   <li>Waits until a connection is ready to read or write, a task is submitted, or a periodic check is due.
 *   <li>Reads (and handles the responses from) every connection that's readable, and writes what's waiting
 *       for every connection that's writable again.
 *   <li>Runs the tasks submitted by other threads (for example, writing a request another thread sent).
 *   <li>Writes the requests sent on this thread during the pass (for example, by a response callback that sends
 *       the next request), to whichever connection of this loop they're for: once per connection, however many
 *       requests it got.
 *   <li>Runs each connection's periodic check when it's due (see {@link KeyValueIdleDetector}).
 * </ol>
 * The loops belong to a {@link KeyValueEventLoopGroup}, which an environment's NIO KV connections share.
 * Each loop's thread runs until the group shuts it down (see {@link #shutdown}).
 * <p>
 * Thread-safe, except where a method says it's for the loop's thread.
 */
@NullMarked
final class KeyValueEventLoop {
  private static final Logger log = LoggerFactory.getLogger(KeyValueEventLoop.class);

  private final Thread thread;
  private final Selector selector;

  /**
   * Tasks submitted by other threads (or by this one, to run later in the pass).
   */
  private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

  /**
   * Whether a thread has woken the selector since the loop last checked for tasks, so other threads needn't.
   */
  private final AtomicBoolean wakeupPending = new AtomicBoolean();

  // Only accessed by the loop's thread.

  /**
   * Connections with requests sent on this thread during the pass, to write at the end of it.
   */
  private final ArrayDeque<NioKeyValueConnection> toFlush = new ArrayDeque<>();

  /**
   * The connections registered with this loop, for their periodic checks.
   */
  private final List<NioKeyValueConnection> connections = new ArrayList<>();

  /**
   * When the next connection's periodic check is due, in {@link System#nanoTime()} terms.
   */
  private long nextCheckNanos;

  // Shutting down: once asked to shut down, the loop closes its connections, and stops once they're done
  // (see run()). Then it's terminated: tasks submitted from then on run on the submitting thread.
  private volatile boolean shutdownRequested;
  private volatile boolean terminated;
  private final CountDownLatch terminatedLatch = new CountDownLatch(1);

  /**
   * Starts the loop's thread (a daemon thread, with the given name).
   */
  KeyValueEventLoop(String name) {
    try {
      this.selector = Selector.open();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open a selector for the KV event loop", e);
    }
    this.nextCheckNanos = System.nanoTime() + TimeUnit.DAYS.toNanos(1);
    this.thread = new Thread(this::run, name);
    this.thread.setDaemon(true);
    this.thread.start();
  }

  boolean inEventLoop() {
    return Thread.currentThread() == thread;
  }

  /**
   * Runs the task on the loop's thread: later in the current pass if called from that thread, or soon otherwise.
   */
  void execute(Runnable task) {
    tasks.add(task);
    // Checked after adding the task, and the loop sets the flag before it runs the last tasks, so either
    // the loop runs this task, or this thread does.
    if (terminated) {
      runLeftoverTasks();
      return;
    }
    if (!inEventLoop() && wakeupPending.compareAndSet(false, true)) {
      selector.wakeup(); // does nothing if the loop has just closed the selector
    }
  }

  /**
   * Writes the connection's queued requests: at the end of the current pass if called from the loop's thread
   * (so the requests sent during the pass are written together), or soon otherwise.
   * The connection makes sure it's scheduled once at a time.
   */
  void scheduleFlush(NioKeyValueConnection connection) {
    if (inEventLoop()) {
      toFlush.add(connection);
    } else {
      execute(connection.flushTask);
    }
  }

  /**
   * Registers the connection's channel with this loop's selector, interested in reading.
   * Call from the loop's thread.
   */
  SelectionKey register(NioKeyValueConnection connection, SocketChannel channel) throws IOException {
    if (shutdownRequested) {
      throw new IOException("The KV event loop is shut down");
    }
    SelectionKey key = channel.register(selector, SelectionKey.OP_READ, connection);
    connections.add(connection);
    long now = System.nanoTime();
    connection.nextCheckNanos = now + connection.checkIntervalNanos();
    if (connection.nextCheckNanos - nextCheckNanos < 0) {
      nextCheckNanos = connection.nextCheckNanos;
    }
    return key;
  }

  /**
   * Stops the periodic checks of a connection that's done. Call from the loop's thread.
   * (Cancelling its key, which the connection does, stops the selector selecting it.)
   */
  void deregister(NioKeyValueConnection connection) {
    connections.remove(connection);
  }

  /**
   * Closes the loop's connections (their requests are retried, as when any connection closes), then stops the loop.
   * New connections can't register from now on.
   */
  void shutdown() {
    shutdownRequested = true;
    execute(() -> {
      for (NioKeyValueConnection connection : connections.toArray(new NioKeyValueConnection[0])) {
        connection.close();
      }
    });
  }

  /**
   * Waits until the loop has stopped (see {@link #shutdown}).
   *
   * @return false if the timeout passed first.
   */
  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return terminatedLatch.await(timeout, unit);
  }

  private void run() {
    while (!(shutdownRequested && connections.isEmpty() && tasks.isEmpty() && toFlush.isEmpty())) {
      try {
        runPass();
      } catch (Throwable t) {
        // A connection's failure closes it (see NioKeyValueConnection); anything else mustn't stop the loop.
        log.warn("Unexpected failure in KV event loop {}", thread.getName(), t);
      }
    }

    terminated = true;
    runLeftoverTasks();
    try {
      selector.close();
    } catch (IOException e) {
      log.debug("Failed to close the selector of KV event loop {}", thread.getName(), e);
    }
    terminatedLatch.countDown();
  }

  /**
   * Runs the tasks submitted after the loop stopped (or just before), on the calling thread, one at a time.
   */
  private synchronized void runLeftoverTasks() {
    Runnable task;
    while ((task = tasks.poll()) != null) {
      try {
        task.run();
      } catch (Throwable t) {
        log.warn("Unexpected failure in a task of stopped KV event loop {}", thread.getName(), t);
      }
    }
  }

  private void runPass() throws IOException {
    // Cleared before checking for tasks, and other threads add a task before waking the selector, so either
    // this sees the task, or the other thread wakes the selector (maybe before select(), which then doesn't wait).
    wakeupPending.set(false);
    if (!tasks.isEmpty() || !toFlush.isEmpty()) {
      selector.selectNow();
    } else {
      long timeoutMillis = TimeUnit.NANOSECONDS.toMillis(nextCheckNanos - System.nanoTime());
      if (timeoutMillis > 0) {
        selector.select(timeoutMillis);
      } else {
        selector.selectNow();
      }
    }

    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
    while (keys.hasNext()) {
      SelectionKey key = keys.next();
      keys.remove();
      ((NioKeyValueConnection) key.attachment()).onSelected(key);
    }

    runTasks();
    flushAll();

    if (System.nanoTime() - nextCheckNanos >= 0) {
      runPeriodicChecks();
      flushAll(); // for example, a NOOP
    }
  }

  private void runTasks() {
    // Only the tasks already submitted: ones submitted meanwhile (for example, by a task) wait for the next pass,
    // so other threads' tasks can't keep this pass from reaching the selector.
    for (int i = tasks.size(); i > 0; i--) {
      Runnable task = tasks.poll();
      if (task == null) {
        break;
      }
      try {
        task.run();
      } catch (Throwable t) {
        log.warn("Unexpected failure in a task of KV event loop {}", thread.getName(), t);
      }
    }
  }

  private void flushAll() {
    NioKeyValueConnection connection;
    while ((connection = toFlush.poll()) != null) {
      connection.flush();
    }
  }

  private void runPeriodicChecks() {
    long now = System.nanoTime();
    long next = now + TimeUnit.DAYS.toNanos(1);
    // A copy, since a check can close (and deregister) its connection.
    for (NioKeyValueConnection connection : connections.toArray(new NioKeyValueConnection[0])) {
      if (now - connection.nextCheckNanos >= 0) {
        connection.periodicCheck();
        connection.nextCheckNanos = now + connection.checkIntervalNanos();
      }
      if (connection.nextCheckNanos - next < 0) {
        next = connection.nextCheckNanos;
      }
    }
    nextCheckNanos = next;
  }

  @Override
  public String toString() {
    return "KeyValueEventLoop{" + thread.getName() + "}";
  }
}
