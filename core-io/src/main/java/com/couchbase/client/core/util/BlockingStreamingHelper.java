/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

@Stability.Internal
public class BlockingStreamingHelper {

  private BlockingStreamingHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final Object STREAM_END_SENTINEL = new Object();

  public static CancellationException propagateAsCancellation(InterruptedException e) {
    // We don't own the thread's interruption policy, so preserve the "interrupted" flag per
    // Java Concurrency In Practice section 7.1.13:
    // """
    // Only code that implements a thread's interruption policy may swallow an interruption request.
    // General-purpose task and library code should never swallow interruption requests.
    // """
    Thread.currentThread().interrupt();

    CancellationException t = new CancellationException("Thread was interrupted.");
    t.addSuppressed(e);
    return t;
  }

  private static <T> void putUninterruptibly(BlockingQueue<T> queue, T value) {
    boolean wasInterrupted = Thread.interrupted();
    while (true) {
      try {
        queue.put(value);
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
        return;
      } catch (InterruptedException e) {
        wasInterrupted = true;
      }
    }
  }

  /**
   * Funnels the contents of a Flux into a queue.
   * Sends backpressure by delaying items until
   * they are removed from the queue and processed
   * by the supplied callback, which is executed
   * in the thread that called this method.
   *
   * @param flux items to process
   * @param buffer number of items to buffer before sending them to the callback.
   * Larger values might improve performance by reducing scheduler context switches.
   * @param callback a callback invoked once for every item in the flux.
   * It is guaranteed to be invoked by the same thread that called this method.
   * @throws CancellationException if the calling thread is interrupted while
   * processing the items.
   */
  public static <T> void forEachBlocking(
    Flux<T> flux,
    int buffer,
    Consumer<T> callback
  ) {
    // Instead of limiting the queue's capacity, use an external semaphore.
    // This lets us enqueue control signals without blocking.
    Semaphore semaphore = new Semaphore(1);
    BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

    Disposable subscription = flux
      .buffer(buffer)  // Reduce scheduler hopping overhead.
      .delayUntil(item -> // Backpressure! Wait for consumer before emitting next batch.
        Mono.fromRunnable(() -> {
            try {
              // Park here until consumer takes the previous item from the queue
              // or cancels the subscription.
              semaphore.acquire();
              queue.put(item);

            } catch (InterruptedException weGotCancelled) {
              // Catch this, so it doesn't go to the onErrorDropped handler.

              // PARANOIA: Release a permit so future iterations (can they exist?) don't deadlock.
              // In any case, we're stopping soon, so it's fine to release a permit
              // even if we were interrupted while acquiring one.
              semaphore.release();

              Thread.currentThread().interrupt(); // Because we don't own the thread's interrupt policy
            }
          })
          .subscribeOn(Schedulers.boundedElastic()) // Because this Mono blocks.
      )

      // PARANOIA: Tell the consumer we got cancelled, even though I'm not sure who but the consumer could have cancelled us.
      .doOnCancel(() -> putUninterruptibly(queue, new CancellationException()))

      .subscribe(
        item -> {
          // Nothing to do here, since the item was processed by the `delayUntil` operator.
        },
        error -> putUninterruptibly(queue, error),
        () -> putUninterruptibly(queue, STREAM_END_SENTINEL)
      );

    try {
      while (true) {
        Object item;
        try {
          item = queue.take();
          semaphore.release();

        } catch (InterruptedException e) {
          throw propagateAsCancellation(e);
        }

        if (item instanceof List) {
          @SuppressWarnings("unchecked")
          List<T> list = (List<T>) item;
          list.forEach(callback);
          continue;
        }

        if (item == STREAM_END_SENTINEL) {
          return;
        }

        if (item instanceof Throwable) {
          throw Exceptions.propagate((Throwable) item);
        }

        throw new RuntimeException("Please report this bug in the SDK; Unexpected item in queue: " + item);
      }

    } finally {
      // Ensure publisher stops, even if consumer exits exceptionally without consuming all items.
      subscription.dispose();

      // Empirically, the publisher gets interrupted because it's hosted by
      // an elastic scheduler, and subsequent attempts by the publisher
      // to acquire a permit / enqueue an item throw InterruptedException.
      // However, I'm not certain whether this is by contract or by coincidence.
      // PARANOIA: Avoid all possibility of deadlock. Deactivate the semaphore by
      // flooding it with more than enough permits for the publisher to finish stopping.
      semaphore.release(Integer.MAX_VALUE / 2);
    }
  }
}
