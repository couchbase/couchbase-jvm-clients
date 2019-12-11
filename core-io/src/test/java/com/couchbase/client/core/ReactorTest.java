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

import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.msg.kv.NoopResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the behavior of the {@link Reactor} utility class methods.
 *
 * @since 2.0.0
 */
class ReactorTest {

  @Test
  void completesWithSuccessAfterSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
      mock(RetryStrategy.class), mock(CollectionIdentifier.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    NoopResponse response = mock(NoopResponse.class);
    StepVerifier verifier = StepVerifier.create(mono).expectNext(response).expectComplete();

    request.succeed(response);
    verifier.verify();
  }

  @Test
  void completesWithErrorAfterSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
      mock(RetryStrategy.class), mock(CollectionIdentifier.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    RequestCanceledException exception = mock(RequestCanceledException.class);
    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);

    request.fail(exception);
    verifier.verify();
  }

  @Test
  void completesWithSuccessBeforeSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
      mock(RetryStrategy.class), mock(CollectionIdentifier.class));
    NoopResponse response = mock(NoopResponse.class);
    request.succeed(response);
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    StepVerifier verifier = StepVerifier.create(mono).expectNext(response).expectComplete();
    verifier.verify();
  }

  @Test
  void completesWithErrorBeforeSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
      mock(RetryStrategy.class), mock(CollectionIdentifier.class));
    RequestCanceledException exception = mock(RequestCanceledException.class);
    request.fail(exception);
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);
    verifier.verify();
  }

  @Test
  void propagatesCancellation() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
      mock(RetryStrategy.class), mock(CollectionIdentifier.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    assertThrows(Exception.class, () -> mono.timeout(Duration.ofMillis(10)).block());
    assertTrue(request.response().isCompletedExceptionally());
    assertTrue(request.response().isDone());
  }

  @Test
  void ignoresCancellationPropagation() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
      mock(RetryStrategy.class), mock(CollectionIdentifier.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), false);

    assertThrows(Exception.class, () -> mono.timeout(Duration.ofMillis(10)).block());
    assertFalse(request.response().isCompletedExceptionally());
    assertFalse(request.response().isDone());
  }

  /**
   * The DirectProcessor is non thread-safe, this is tested in the next test.
   */
  @Test
  void directProcessorHasExpectedSemantics() {
    DirectProcessor<Integer> processor = DirectProcessor.create();

    processor.onNext(1);

    List<Integer> consumed = new ArrayList<>();
    processor.subscribe(consumed::add);

    processor.onNext(2);
    processor.onNext(3);

    assertEquals(2, consumed.size());
    assertEquals(2, (int) consumed.get(0));
    assertEquals(3, (int) consumed.get(1));
  }

  @Test
  void threadSafeSinkWorksForDirectProcessor() {
    ExecutorService service = Executors.newCachedThreadPool();
    DirectProcessor<Integer> processor = DirectProcessor.create();
    final FluxSink<Integer> sink = processor.sink();

    List<Integer> consumed = new ArrayList<>();
    processor.subscribe(consumed::add);

    int actors = 16;
    for (int i = 0; i < actors; i++) {
      final int v = i;
      service.submit(() -> {
        sink.next(v);
      });
    }

    waitUntilCondition(() -> consumed.size() == actors);
    assertEquals(actors, consumed.size());

    service.shutdownNow();

  }

  @Test
  void noErrorDroppedWhenCancelledViaCompletionException() {
    AtomicInteger droppedErrors = new AtomicInteger(0);
    Hooks.onErrorDropped(v -> {
      droppedErrors.incrementAndGet();
    });

    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
            mock(RetryStrategy.class), mock(CollectionIdentifier.class));

    // Because this is a multi-step CompleteableFuture, the RequestCanceledException will be wrapped in a
    // CompletionException in the internals.  It will be unwrapped by the time it is raised to the app.
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response().thenApply(v -> v), true);

    Disposable subscriber = mono.subscribe();

    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);

    subscriber.dispose();

    verifier.verify();
    assertEquals(0, droppedErrors.get());
  }


  @Test
  void noErrorDroppedWhenCancelledViaRequestCanceledException() {
    AtomicInteger droppedErrors = new AtomicInteger(0);
    Hooks.onErrorDropped(v -> {
      droppedErrors.incrementAndGet();
    });

    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class),
            mock(RetryStrategy.class), mock(CollectionIdentifier.class));

    // Because this is a single-stage CompleteableFuture, the RequestCanceledException will raised directly in the
    // internals.
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    Disposable subscriber = mono.subscribe();

    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);

    subscriber.dispose();

    verifier.verify();
    assertEquals(0, droppedErrors.get());
  }
}