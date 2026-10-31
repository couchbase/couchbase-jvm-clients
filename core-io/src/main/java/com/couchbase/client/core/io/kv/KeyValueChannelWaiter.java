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

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Waits, until a deadline, for a non-blocking channel to connect, or to be readable or writable,
 * with a selector of its own. So a connect attempt (TCP, TLS and the KV handshake) can be written as blocking code,
 * with the connect timeout, before the connection is handed to an event loop (see {@link NioKeyValueConnection}).
 * <p>
 * Used by the connecting thread, except {@link #wakeup}, which any thread can call.
 */
@NullMarked
final class KeyValueChannelWaiter implements Closeable {
  private final SocketChannel channel;
  private final Selector selector;
  private final SelectionKey key;

  KeyValueChannelWaiter(SocketChannel channel) throws IOException {
    this.channel = requireNonNull(channel);
    this.selector = Selector.open();
    try {
      this.key = channel.register(selector, 0);
    } catch (IOException | RuntimeException e) {
      selector.close();
      throw e;
    }
  }

  /**
   * Waits until the channel is ready for one of the operations (for example, {@link SelectionKey#OP_READ}).
   * Might return early (for example, after {@link #wakeup}); the caller tries the operation, and waits again if it
   * isn't ready.
   *
   * @throws SocketTimeoutException if the deadline passes first.
   * @throws ClosedChannelException if the channel was closed (for example, to stop the connect attempt).
   */
  void await(int ops, long deadlineNanos) throws IOException {
    if (!channel.isOpen()) {
      throw new ClosedChannelException();
    }
    long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      // The same messages as a blocking socket's.
      throw new SocketTimeoutException(ops == SelectionKey.OP_READ ? "Read timed out" : "Timed out");
    }
    key.interestOps(ops);
    try {
      selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
    } finally {
      selector.selectedKeys().clear();
    }
    if (Thread.interrupted()) {
      // Like a blocking socket read: the attempt fails. (Nothing in the SDK interrupts the connecting thread.)
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while connecting");
    }
    if (!channel.isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Makes a waiting {@link #await} return (for example, because the channel was closed to stop the attempt).
   */
  void wakeup() {
    selector.wakeup();
  }

  /**
   * Returns true if the waiter's selector is closed. For tests.
   */
  boolean isClosed() {
    return !selector.isOpen();
  }

  /**
   * Stops using the channel with this waiter's selector (so it can be registered with an event loop).
   */
  @Override
  public void close() throws IOException {
    key.cancel();
    selector.close();
  }
}
