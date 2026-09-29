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
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bridges a single blocking producer thread to a {@link Flux} of items.
 * <p>
 * Behaviour:
 * <ul>
 *   <li>Before the flux is subscribed, {@link #emitNext} never blocks; items are buffered without bound.
 *   <li>Once subscribed, {@link #emitNext} blocks while the number of items buffered but not yet taken
 *       by downstream is at or above {@code highWaterMark}, and resumes once it has fallen to
 *       {@code highWaterMark / 2}. The pre-subscription backlog counts toward this.
 *   <li>After the subscriber cancels, {@link #emitNext} never blocks and drops its argument,
 *       so the producer can read through to the end of the input and call {@link #complete}.
 *   <li>{@link #complete} and {@link #fail} terminate both the flux and the mono. The mono is completed
 *       whether or not anyone ever subscribed to the flux.
 *   <li>Interrupting the producer thread fails both the flux and the mono with the
 *       {@link InterruptedException}. Only waits inside {@link #emitNext} observe interruption; a
 *       thread blocked inside {@code InputStream.read()} will not notice it until the read returns.
 *   <li>At most one subscriber. A second subscriber receives {@code onError(IllegalStateException)}.
 * </ul>
 * {@link #emitNext}, {@link #complete} and {@link #fail} must only be called from the producer thread.
 * {@link #rows()} may be used from any thread.
 *
 * @param <T> item type
 */
@NullMarked
public final class BlockingStreamBridge<T> {

  private final int highWaterMark;
  private final int lowWaterMark;

  private final CountingQueue queue;
  private final Sinks.Many<T> rowSink;
  private final Flux<T> rows;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition drained = lock.newCondition();

  /**
   * True while the producer is (about to be) parked waiting for the buffer to drain.
   */
  private volatile boolean producerWaiting;
  /**
   * Set once the (single) subscriber has subscribed. Never reset.
   */
  private volatile boolean subscribed;
  /**
   * Set once the subscriber has cancelled. Never reset.
   */
  private volatile boolean cancelled;
  /**
   * Set by the producer immediately before it terminates the row sink.
   */
  private volatile boolean terminatedByProducer;

  // Producer-thread-only state.
  private boolean terminated;

  /**
   * @param highWaterMark buffered-item count at which the producer blocks once subscribed; must be >= 1
   */
  public BlockingStreamBridge(int highWaterMark) {
    if (highWaterMark < 1) {
      throw new IllegalArgumentException("highWaterMark must be >= 1, was " + highWaterMark);
    }
    this.highWaterMark = highWaterMark;
    this.lowWaterMark = highWaterMark / 2;
    this.queue = new CountingQueue(Queues.<T>unbounded().get());

    // The end callback runs exactly once, on cancellation or on termination, whichever happens first.
    this.rowSink = Sinks.many().unicast().onBackpressureBuffer(queue, this::onRowSinkEnded);

    // Flux.defer passes the subscriber straight through to the sink, so operators downstream
    // (notably publishOn) can still fuse with it and take items directly from the queue above.
    // A peek operator such as doOnSubscribe here would prevent that fusion.
    this.rows = Flux.defer(() -> {
      subscribed = true;
      return rowSink.asFlux();
    });
  }

  /**
   * The item flux. Supports exactly one subscriber.
   */
  public Flux<T> rows() {
    return rows;
  }
  
  /**
   * True once the subscriber has cancelled. The producer can use this to skip decoding items
   * that would only be dropped.
   */
  public boolean isCancelled() {
    return cancelled;
  }

  /**
   * Publishes one item, blocking if backpressure currently applies.
   *
   * @return {@code true} if the item was accepted for delivery, {@code false} if it was dropped
   * because the subscriber has cancelled
   * @throws InterruptedException if the thread is interrupted on entry or while waiting; the flux and
   * mono have already been failed when this is thrown
   * @throws IllegalStateException if called after {@link #complete} or {@link #fail}
   */
  public boolean emitNext(T item) throws InterruptedException {
    Objects.requireNonNull(item, "item");
    if (terminated) {
      throw new IllegalStateException("emitNext called after termination");
    }
    if (Thread.interrupted()) {
      throw failOnInterrupt(new InterruptedException());
    }
    if (cancelled) {
      return false;
    }

    try {
      awaitCapacity();
    } catch (InterruptedException e) {
      throw failOnInterrupt(e);
    }

    Sinks.EmitResult result = rowSink.tryEmitNext(item);
    if (result.isSuccess()) {
      return true;
    }
    if (result == Sinks.EmitResult.FAIL_CANCELLED) {
      cancelled = true;
      return false;
    }
    // FAIL_TERMINATED is excluded by the check above, FAIL_OVERFLOW by the unbounded queue,
    // and FAIL_NON_SERIALIZED by the single-producer contract. Reaching here is a bug.
    throw new IllegalStateException("Unexpected emit result: " + result);
  }

  /**
   * Completes the flux, then completes the mono with {@code metadata} (or empty if null).
   *
   * @throws IllegalStateException if already terminated
   */
  public void complete() {
    if (terminated) {
      throw new IllegalStateException("Already terminated");
    }
    terminated = true;
    terminatedByProducer = true;
    rowSink.tryEmitComplete(); // FAIL_CANCELLED is fine: nobody is listening.
  }

  /**
   * Fails the flux (after any buffered items are delivered) and the mono. No-op if already
   * terminated, so it is safe to call from a catch block after {@link #emitNext} threw.
   */
  public void fail(Throwable error) {
    Objects.requireNonNull(error, "error");
    if (terminated) {
      return;
    }
    terminated = true;
    terminatedByProducer = true;
    rowSink.tryEmitError(error);
  }

  private InterruptedException failOnInterrupt(InterruptedException e) {
    fail(e);
    return e;
  }

  private boolean backpressureApplies() {
    return subscribed && !cancelled;
  }

  /*
   * Why an untimed await cannot miss a wake-up. The producer waits while
   * (count > lowWaterMark && subscribed && !cancelled), and each way that condition can become
   * false is paired with a signal:
   *
   * - Count falls to lowWaterMark. Every item leaves the queue through CountingQueue.poll(), since
   *   the sink and fused operators only ever poll, and clear() is inherited from AbstractQueue,
   *   which polls. The producer writes producerWaiting and then reads count; the consumer
   *   decrements count and then reads producerWaiting. Both are sequentially consistent, so either
   *   the producer sees the new count and does not wait, or the consumer sees the flag and signals.
   *
   * - Cancellation. onRowSinkEnded writes cancelled and then signals under the lock. The producer
   *   checks cancelled while holding that lock and only releases it inside await(), so the signal
   *   either precedes the check (and the check sees the flag) or reaches the waiting producer.
   *
   * - subscribed never goes from true to false, and the producer never waits before it is true.
   *
   * Neither signalling path can deadlock against the producer: the producer holds the lock only
   * while checking the condition or parked in await(), and never calls into the sink with it held.
   */
  private void awaitCapacity() throws InterruptedException {
    if (queue.count() < highWaterMark || !backpressureApplies()) {
      return; // fast path, no locking
    }
    lock.lockInterruptibly();
    try {
      producerWaiting = true;
      while (queue.count() > lowWaterMark && backpressureApplies()) {
        drained.await();
      }
    } finally {
      producerWaiting = false;
      lock.unlock();
    }
  }

  private void wakeProducer() {
    lock.lock();
    try {
      drained.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void onRowSinkEnded() {
    if (!terminatedByProducer) {
      cancelled = true;
      wakeProducer();
    }
  }

  /**
   * Package-private for tests: items buffered but not yet taken by downstream.
   */
  int bufferedCount() {
    return queue.count();
  }

  /**
   * Wraps the sink's queue to keep an exact count and to wake the producer as items are taken.
   * Offered on the producer thread; polled by whichever thread is draining the sink.
   */
  private final class CountingQueue extends AbstractQueue<T> {

    private final Queue<T> delegate;
    private final AtomicInteger count = new AtomicInteger();

    CountingQueue(Queue<T> delegate) {
      this.delegate = delegate;
    }

    int count() {
      return count.get();
    }

    @Override
    public boolean offer(T t) {
      count.incrementAndGet(); // before the offer, so a racing poll can never drive it negative
      if (delegate.offer(t)) {
        return true;
      }
      count.decrementAndGet();
      return false;
    }

    @Override
    public @Nullable T poll() {
      T t = delegate.poll();
      if (t != null && count.decrementAndGet() <= lowWaterMark && producerWaiting) {
        wakeProducer();
      }
      return t;
    }

    @Override
    public @Nullable T peek() {
      return delegate.peek();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public int size() {
      return count.get();
    }

    /**
     * Not supported. This also makes the inherited {@code toString}, {@code contains} and
     * {@code remove(Object)} throw; none of them is used by Reactor on this queue.
     */
    @Override
    public Iterator<T> iterator() {
      throw new UnsupportedOperationException("CountingQueue does not support iteration");
    }

    @Override
    public String toString() {
      return "CountingQueue{" +
        "count=" + count +
        '}';
    }
  }
}
