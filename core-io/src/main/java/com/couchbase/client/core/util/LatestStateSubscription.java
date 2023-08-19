/*
 * Copyright 2023 Couchbase, Inc.
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
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.NonNull;

import static java.util.Objects.requireNonNull;

/**
 * Useful when only the latest value of a Flux is interesting,
 * and processing values is relatively expensive.
 * <p>
 * Think of it as applying a hypothetical "sampleLatest" operator
 * to the upstream flux before subscribing. In other words,
 * it ignores all but the latest value.
 * <p>
 * Only one value is processed at a time. The processing happens
 * asynchronously with respect to the Flux.
 * <p>
 * Requires collaboration with the subscriber; after processing
 * each value, the subscriber must run a callback to signal
 * readiness for the next value.
 */
@Stability.Internal
public class LatestStateSubscription<T> {

  @FunctionalInterface
  public interface AsyncSubscriber<T> {
    /**
     * Handles a value, typically by launching an asynchronous task.
     * Must run {@code doFinally} when the task is complete.
     *
     * @param value Most recent value from the upstream Flux.
     * @param doFinally Callback to run after asynchronous processing is complete.
     */
    void hookOnNext(T value, Runnable doFinally);
  }

  private final Sinks.One<Void> terminationSignal = Sinks.one();
  private final Scheduler scheduler;
  private final AsyncSubscriber<T> subscriber;

  /**
   * Guards all non-final fields.
   */
  private final Object lock = new Object();

  private boolean upstreamTerminated;
  private boolean processingInProgress;
  private T deferredValue;

  public LatestStateSubscription(
    Flux<T> flux,
    Scheduler scheduler,
    AsyncSubscriber<T> subscriber
  ) {
    this.scheduler = requireNonNull(scheduler);
    this.subscriber = requireNonNull(subscriber);

    flux
      .onBackpressureLatest()
      .publishOn(scheduler)
      .subscribe(
        new BaseSubscriber<T>() {
          @Override
          protected void hookOnNext(@NonNull T value) {
            synchronized (lock) {
              if (processingInProgress) {
                deferredValue = value;
                return;
              }
              process(value);
            }
          }

          @Override
          protected void hookFinally(@NonNull SignalType type) {
            synchronized (lock) {
              upstreamTerminated = true;
              if (!processingInProgress) {
                terminationSignal.tryEmitEmpty().orThrow();
              }
            }
          }
        }
      );
  }

  private void process(T value) {
    processingInProgress = true;
    scheduler.schedule(() -> subscriber.hookOnNext(value, this::onFinishedProcessingValue));
  }

  private void onFinishedProcessingValue() {
    synchronized (lock) {
      if (deferredValue != null) {
        T value = deferredValue;
        deferredValue = null;
        process(value);
        return;
      }

      processingInProgress = false;
      if (upstreamTerminated) {
        terminationSignal.tryEmitEmpty().orThrow();
      }
    }
  }

  /**
   * Returns a Mono that completes after the upstream flux terminates
   * and its final value is completely processed.
   */
  public Mono<Void> awaitTermination() {
    return terminationSignal.asMono();
  }
}
