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
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * A {@link KeyValueConnection}'s requests waiting to be written, and how senders wake its writer thread.
 * <ul>
 *   <li>Writability, like Netty's channel writability: once more than the high water mark of bytes is waiting,
 *       the connection isn't writable until less than the low water mark is. Requests are queued unencoded
 *       (the writer encodes them), so each counts as {@value #UNENCODED_REQUEST_SIZE} bytes, like a write the Netty
 *       implementation's event loop hasn't encoded yet.
 *   <li>Waking the writer: a sender wakes it only if it's parked (see {@link #awaitRequests}). Requests sent by
 *       the reader thread while it's handling a batch of responses don't wake it; the reader wakes it once,
 *       when it finishes the batch (see {@link #endReadBatch}), so they're written together
 *       (like the Netty implementation's FlushConsolidationHandler). A long batch (for example, one that keeps
 *       going because each read ends partway through a packet) wakes the writer every so many requests,
 *       so they don't wait for the batch to end.
 *   <li>Closing: {@link #close} tells the writer to stop. Once the connection's threads are done,
 *       {@link #drain} returns what was never written, and from then on, {@link #add} refuses requests,
 *       so none is left behind.
 * </ul>
 * Thread-safe, except where a method says which thread calls it.
 */
@NullMarked
final class KeyValueOutboundQueue {

  /**
   * A request waiting to be written.
   */
  static final class Outbound {
    private final @Nullable KeyValueRequest<Response> request; // null only for CLOSE
    final int size;

    /**
     * @param size how many bytes it counts as, for writability.
     */
    Outbound(@Nullable KeyValueRequest<Response> request, int size) {
      this.request = request;
      this.size = size;
    }

    KeyValueRequest<Response> request() {
      return requireNonNull(request);
    }
  }

  /**
   * Tells the writer to stop.
   */
  static final Outbound CLOSE = new Outbound(null, 0);

  /**
   * How many bytes a queued (not yet encoded) request counts as, for writability: like the Netty implementation,
   * where a write submitted from outside the event loop counts as its message size estimate (8 bytes for a request)
   * plus the write task overhead (32 bytes), until the event loop encodes it.
   */
  static final int UNENCODED_REQUEST_SIZE = 40;

  private final int highWaterMark;
  private final int lowWaterMark;
  private final int maxDeferredWakes;

  private final ConcurrentLinkedQueue<Outbound> queue = new ConcurrentLinkedQueue<>();
  private final AtomicLong queuedBytes = new AtomicLong();
  private volatile boolean writable = true;

  private volatile @Nullable Thread readerThread;
  private volatile @Nullable Thread writerThread;
  private volatile boolean writerParked;
  private volatile boolean closed;
  private volatile boolean drained;

  // Only accessed by the reader thread: whether it's handling responses it has already read,
  // and how many requests it sent in the meantime without waking the writer.
  private boolean handlingReadBatch;
  private int deferredWakes;

  /**
   * @param maxDeferredWakes how many requests the reader thread can send while handling a batch of responses
   * before it wakes the writer anyway (like the Netty implementation's FlushConsolidationHandler's
   * explicitFlushAfterFlushes).
   */
  KeyValueOutboundQueue(int highWaterMark, int lowWaterMark, int maxDeferredWakes) {
    if (maxDeferredWakes < 1) {
      throw new IllegalArgumentException("maxDeferredWakes must be positive, but got " + maxDeferredWakes);
    }
    this.highWaterMark = highWaterMark;
    this.lowWaterMark = lowWaterMark;
    this.maxDeferredWakes = maxDeferredWakes;
  }

  /**
   * Returns false if too many bytes are waiting to be written.
   */
  boolean isWritable() {
    return writable;
  }

  /**
   * Queues the request to be written, and wakes the writer (unless the reader thread is handling a batch).
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

    if (drained && queue.remove(outbound)) {
      return false;
    }

    if (Thread.currentThread() == readerThread && handlingReadBatch) {
      if (++deferredWakes < maxDeferredWakes) {
        return true; // the reader wakes the writer when it finishes the batch
      }
      deferredWakes = 0; // the batch goes on, so waking it now (for the deferred requests too)
    }
    wakeWriter();
    return true;
  }

  /**
   * Accounts for a request that was taken off the queue (written, or failed).
   */
  void written(Outbound outbound) {
    if (queuedBytes.addAndGet(-outbound.size) < lowWaterMark) {
      writable = true;
    }
  }

  // ---- the writer

  /**
   * Call from the writer thread, before anything else.
   */
  void writerStarted() {
    writerThread = Thread.currentThread();
  }

  /**
   * Returns the next request to write ({@link #CLOSE} once the queue is closed), or null if there's none.
   * Call from the writer thread.
   */
  @Nullable Outbound poll() {
    return queue.poll();
  }

  /**
   * Waits until a request is added, or the queue is closed (returns at once if there's already one, or it's
   * already closed). Might also return with nothing to write (after a leftover or spurious wake-up); the caller
   * checks again. Call from the writer thread.
   */
  void awaitRequests() {
    // Sets the flag before checking the queue one last time, and senders add to the queue before checking the flag,
    // so either this sees the request, or the sender sees the flag (and wakes the writer, maybe before it parks:
    // then it doesn't park).
    writerParked = true;
    if (queue.isEmpty() && !closed) {
      LockSupport.park(this);
    }
    writerParked = false;
  }

  /**
   * Wakes the writer if requests are waiting to be written (for example, ones the reader sent without waking it,
   * while it handles a batch of responses).
   */
  void wakeWriterIfNotEmpty() {
    if (!queue.isEmpty()) {
      wakeWriter();
    }
  }

  private void wakeWriter() {
    if (writerParked) {
      LockSupport.unpark(writerThread);
    }
  }

  // ---- the reader

  /**
   * Call from the reader thread, before anything else.
   */
  void readerStarted() {
    readerThread = Thread.currentThread();
  }

  /**
   * Call from the reader thread before handling each response: requests it sends while handling them don't wake
   * the writer, until {@link #endReadBatch}.
   */
  void beginReadBatch() {
    handlingReadBatch = true;
  }

  /**
   * Call from the reader thread when it has handled every response it already read. Like the Netty implementation's
   * {@code FlushConsolidationHandler} on {@code channelReadComplete}: wakes the writer once for all the requests
   * sent while handling them.
   */
  void endReadBatch() {
    handlingReadBatch = false;
    if (deferredWakes > 0) {
      deferredWakes = 0;
      wakeWriter();
    }
  }

  // ---- closing

  /**
   * Tells the writer to stop: once it has written what's ahead of it, it takes {@link #CLOSE} off the queue.
   */
  void close() {
    closed = true;
    queue.add(CLOSE);
    LockSupport.unpark(writerThread);
  }

  /**
   * Returns the requests that were never written, and from now on, refuses new ones (see {@link #add}).
   * Call once the connection's threads are done, so nothing else takes requests off the queue.
   */
  List<Outbound> drain() {
    drained = true;
    List<Outbound> unwritten = new ArrayList<>();
    Outbound outbound;
    while ((outbound = queue.poll()) != null) {
      if (outbound != CLOSE) {
        unwritten.add(outbound);
      }
    }
    return unwritten;
  }
}
