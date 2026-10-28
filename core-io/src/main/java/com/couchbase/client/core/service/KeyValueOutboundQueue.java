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

import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * A {@link NioKeyValueConnection}'s requests waiting to be written.
 * <ul>
 *   <li>Writability, like Netty's channel writability: once more than the high water mark of bytes is waiting,
 *       the connection isn't writable until less than the low water mark is. Requests are queued unencoded
 *       (the event loop encodes them), so each counts as {@value #UNENCODED_REQUEST_SIZE} bytes, like a write the
 *       Netty implementation's event loop hasn't encoded yet.
 *   <li>Closing: once the connection is closed, {@link #drain} returns what was never written, and from then on,
 *       {@link #add} refuses requests, so none is left behind.
 * </ul>
 * Thread-safe.
 */
@NullMarked
final class KeyValueOutboundQueue {

  /**
   * A request waiting to be written.
   */
  static final class Outbound {
    private final KeyValueRequest<Response> request;
    final int size;

    /**
     * @param size how many bytes it counts as, for writability.
     */
    Outbound(KeyValueRequest<Response> request, int size) {
      this.request = requireNonNull(request);
      this.size = size;
    }

    KeyValueRequest<Response> request() {
      return request;
    }
  }

  /**
   * How many bytes a queued (not yet encoded) request counts as, for writability: like the Netty implementation,
   * where a write submitted from outside the event loop counts as its message size estimate (8 bytes for a request)
   * plus the write task overhead (32 bytes), until the event loop encodes it.
   */
  static final int UNENCODED_REQUEST_SIZE = 40;

  private final int highWaterMark;
  private final int lowWaterMark;

  private final ConcurrentLinkedQueue<Outbound> queue = new ConcurrentLinkedQueue<>();
  private final AtomicLong queuedBytes = new AtomicLong();
  private volatile boolean writable = true;
  private volatile boolean drained;

  KeyValueOutboundQueue(int highWaterMark, int lowWaterMark) {
    this.highWaterMark = highWaterMark;
    this.lowWaterMark = lowWaterMark;
  }

  /**
   * Returns false if too many bytes are waiting to be written.
   */
  boolean isWritable() {
    return writable;
  }

  /**
   * Queues the request to be written.
   *
   * @return false if the connection's unwritten requests were already {@linkplain #drain drained}, so nobody would
   * write it (the caller should fail it, after {@link #written}).
   */
  boolean add(Outbound outbound) {
    // Before queueing it, so writing it (which comes later) makes the queue writable again:
    // a sender can't leave it unwritable with nothing left to write.
    if (queuedBytes.addAndGet(outbound.size) > highWaterMark) {
      writable = false;
    }
    queue.add(outbound);
    return !(drained && queue.remove(outbound));
  }

  /**
   * Accounts for a request that was taken off the queue (written, or failed).
   */
  void written(Outbound outbound) {
    if (queuedBytes.addAndGet(-outbound.size) < lowWaterMark) {
      writable = true;
    }
  }

  /**
   * Returns the next request to write, or null if there's none.
   */
  @Nullable Outbound poll() {
    return queue.poll();
  }

  /**
   * Returns the requests that were never written, and from now on, refuses new ones (see {@link #add}).
   * Call once the connection is closed, so nothing else takes requests off the queue.
   */
  List<Outbound> drain() {
    drained = true;
    List<Outbound> unwritten = new ArrayList<>();
    Outbound outbound;
    while ((outbound = queue.poll()) != null) {
      unwritten.add(outbound);
    }
    return unwritten;
  }
}
