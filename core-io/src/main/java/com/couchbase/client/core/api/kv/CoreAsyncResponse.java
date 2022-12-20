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

package com.couchbase.client.core.api.kv;


import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.CoreAsyncUtils;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Associates a {@link CompletableFuture} with a cancellation task.
 * <p>
 * Lets the Async API tell the Reactive API how to cancel a request
 * in a protocol-agnostic way, so the default implementation of the
 * Reactive API works with both Classic and Protostellar.
 * <p>
 * A CompletableFuture alone is not sufficient, because a completion stage
 * does not propagate cancellation upstream. If the Async API methods were to return
 * only a CompletableFuture, they could not reliably attach the cancellation logic
 * required by the Reactive API methods (which by default are thin wrappers over the Async APIs).
 */
@Stability.Internal
public final class CoreAsyncResponse<T> {
  private final CompletableFuture<T> future;
  private final Runnable cancellationTask;

  public CoreAsyncResponse(CompletableFuture<T> future, Runnable cancellationTask) {
    this.future = requireNonNull(future);
    this.cancellationTask = requireNonNull(cancellationTask);
  }

  public T toBlocking() {
    return CoreAsyncUtils.block(future);
  }

  public CompletableFuture<T> toFuture() {
    return future;
  }

  public Mono<T> toMono() {
    return Reactor.wrap(future, cancellationTask);
  }

  public <U> CompletableFuture<U> thenApply(Function<? super T,? extends U> fn) {
    return toFuture().thenApply(fn);
  }
}
