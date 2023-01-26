/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * This class provides utility methods when working with reactor.
 *
 * @since 2.0.0
 */
public class Reactor {

  public static final Duration DEFAULT_EMIT_BUSY_DURATION = Duration.ofSeconds(1);

  private Reactor() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Wraps a {@link Request} and returns it in a {@link Mono}.
   *
   * @param request the request to wrap.
   * @param response the full response to wrap, might not be the same as in the request.
   * @param propagateCancellation if a cancelled/unsubscribed mono should also cancel the
   *                              request.
   * @return the mono that wraps the request.
   */
  public static <T> Mono<T> wrap(final Request<?> request, final CompletableFuture<T> response,
                                 final boolean propagateCancellation) {
    Mono<T> mono = MyLittleAssemblyFactory.callOnAssembly(new SilentMonoCompletionStage<>(response));
    if (propagateCancellation) {
      mono = mono.doFinally(st -> {
        if (st == SignalType.CANCEL) {
          request.cancel(CancellationReason.STOPPED_LISTENING);
        }
      });
    }
    return mono.onErrorResume(err -> {
      if (err instanceof CompletionException) {
        return Mono.error(err.getCause());
      } else {
        return Mono.error(err);
      }
    });
  }

  /**
   * Converts the given future into a mono. Runs the given cancellation task
   * when the mono is cancelled.
   */
  @Stability.Internal
  public static <T> Mono<T> wrap(final CompletableFuture<T> response, final Runnable cancellationTask) {
    Mono<T> mono = MyLittleAssemblyFactory.callOnAssembly(new SilentMonoCompletionStage<>(response));

    mono = mono.doFinally(st -> {
      if (st == SignalType.CANCEL) {
        cancellationTask.run();
      }
    });

    return mono.onErrorResume(err -> {
      if (err instanceof CompletionException) {
        return Mono.error(err.getCause());
      } else {
        return Mono.error(err);
      }
    });
  }

  /**
   * Helper method to wrap an async call into a reactive one and translate
   * exceptions appropriately.
   *
   * @param input a supplier that will be called on every subscription.
   * @return a mono that invokes the given supplier on each subscription.
   */
  public static <T> Mono<T> toMono(Supplier<CompletableFuture<T>> input) {
    return Mono.fromFuture(input)
        .onErrorMap(t -> t instanceof CompletionException ? t.getCause() : t);
  }

  /**
   * Helper method to wrap an async call into a reactive one and translate
   * exceptions appropriately.
   *
   * @param input a supplier that will be called on every subscription.
   * @return a flux that invokes the given supplier on each subscription.
   */
  public static <T, C extends Iterable<T>> Flux<T> toFlux(Supplier<CompletableFuture<C>> input) {
    return toMono(input).flux().flatMap(Flux::fromIterable);
  }

  /**
   * Emits the value or error produced by the wrapped CompletionStage.
   * <p>
   * Note that if Subscribers cancel their subscriptions, the CompletionStage
   * is not cancelled.
   * <p>
   * COUCHBASE NOTE: This class is an exact copy from the MonoCompletionStage that ships with reactor. The only changes
   * made to it are that we need to check for a specific exception when the downstream consumer is cancelled. See the
   * reasoning in that codeblock below. The code is copied from reactor-core version 3.3.0.RELEASE.
   *
   * @param <T> the value type
   */
  private static final class SilentMonoCompletionStage<T> extends Mono<T>
    implements Fuseable, Scannable {

    final CompletionStage<? extends T> future;

    SilentMonoCompletionStage(CompletionStage<? extends T> future) {
      this.future = Objects.requireNonNull(future, "future");
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      Operators.MonoSubscriber<T, T>
        sds = new Operators.MonoSubscriber<>(actual);

      actual.onSubscribe(sds);

      if (sds.isCancelled()) {
        return;
      }

      future.whenComplete((v, e) -> {
        if (sds.isCancelled()) {
          //nobody is interested in the Mono anymore, don't risk dropping errors
          Context ctx = sds.currentContext();
          if (e == null || e instanceof CancellationException) {
            //we discard any potential value and ignore Future cancellations
            Operators.onDiscard(v, ctx);
          }
          else {
            //we make sure we keep _some_ track of a Future failure AFTER the Mono cancellation

            // COUCHBASE NOTE: We changed this code because in the base class we explicitly call STOPPED_LISTENING
            // if the downstream consumer closes. Do not call onErrorDropped in this case, since we expect this
            // case to be happening. Default reactor only suppresses this for cancellations, but our exception
            // hierachy doesn't allow for it, hence the workaround.
            // Note: sometimes it's a raw RequestCancelException, sometimes it's wrapped in a CompletionException.
            // See ReactorTest::noErrorDroppedWhenCancelledVia* tests for examples.
            if (e instanceof CompletionException && e.getCause() instanceof RequestCanceledException) {
              RequestContext requestContext = ((RequestCanceledException) (e.getCause())).context().requestContext();
              if (requestContext.request().cancellationReason() != CancellationReason.STOPPED_LISTENING) {
                Operators.onErrorDropped(e, ctx);
              }
            } else if (e instanceof RequestCanceledException) {
              RequestContext requestContext = ((RequestCanceledException) e).context().requestContext();
              if (requestContext.request().cancellationReason() != CancellationReason.STOPPED_LISTENING) {
                Operators.onErrorDropped(e, ctx);
              }
            } else {
              Operators.onErrorDropped(e, ctx);
            }

            //and we discard any potential value just in case both e and v are not null
            Operators.onDiscard(v, ctx);
          }

          return;
        }
        try {
          if (e instanceof CompletionException) {
            actual.onError(e.getCause());
          }
          else if (e != null) {
            actual.onError(e);
          }
          else if (v != null) {
            sds.complete(v);
          }
          else {
            actual.onComplete();
          }
        }
        catch (Throwable e1) {
          Operators.onErrorDropped(e1, actual.currentContext());
          throw Exceptions.bubble(e1);
        }
      });
    }

    @Override
    public Object scanUnsafe(Attr key) {
      return null; //no particular key to be represented, still useful in hooks
    }
  }

  /**
   * We have or own little pony eeeh factory because onAssembly is protected inside the mono, so we need to expose it!
   */
  private abstract static class MyLittleAssemblyFactory<T> extends Mono<T> {
    static <T> Mono<T> callOnAssembly(Mono<T> source) {
      return onAssembly(source);
    }
  }

  /**
   * Constructs a new EmitFailureHandler with the default busy wait duration.
   *
   * @return a new emit failure handler, busy looping for {@link #DEFAULT_EMIT_BUSY_DURATION}.
   */
  public static Sinks.EmitFailureHandler emitFailureHandler() {
    return emitFailureHandler(DEFAULT_EMIT_BUSY_DURATION);
  }

  /**
   * Constructs a new EmitFailureHandler with a custom busy wait duration.
   *
   * @param duration the duration to busy loop until failure.
   * @return a new emit failure handler, busy looping for the given duration.
   */
  public static Sinks.EmitFailureHandler emitFailureHandler(final Duration duration) {
    return Sinks.EmitFailureHandler.busyLooping(duration);
  }

}
