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
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.msg.kv.NoopResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.core.util.MockUtil.mockCoreContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the behavior of the {@link Reactor} utility class methods.
 */
class ReactorTest {

  private static NoopRequest newNoopRequest() {
    return new NoopRequest(
      Duration.ZERO,
      mockCoreContext(mockCore(), RequestContext.class),
      mock(RetryStrategy.class),
      mock(CollectionIdentifier.class)
    );
  }

  @Test
  void completesWithSuccessAfterSubscription() {
    NoopRequest request = newNoopRequest();
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    NoopResponse response = mock(NoopResponse.class);
    StepVerifier verifier = StepVerifier.create(mono).expectNext(response).expectComplete();

    request.succeed(response);
    verifier.verify();
  }

  @Test
  void completesWithErrorAfterSubscription() {
    NoopRequest request = newNoopRequest();
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    RequestCanceledException exception = mock(RequestCanceledException.class);
    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);

    request.fail(exception);
    verifier.verify();
  }

  @Test
  void completesWithSuccessBeforeSubscription() {
    NoopRequest request = newNoopRequest();
    NoopResponse response = mock(NoopResponse.class);
    request.succeed(response);
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    StepVerifier verifier = StepVerifier.create(mono).expectNext(response).expectComplete();
    verifier.verify();
  }

  @Test
  void completesWithErrorBeforeSubscription() {
    NoopRequest request = newNoopRequest();
    RequestCanceledException exception = mock(RequestCanceledException.class);
    request.fail(exception);
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);
    verifier.verify();
  }

  @Test
  void propagatesCancellation() {
    NoopRequest request = newNoopRequest();
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), true);

    assertThrows(Exception.class, () -> mono.timeout(Duration.ofMillis(10)).block());
    assertTrue(request.response().isCompletedExceptionally());
    assertTrue(request.response().isDone());
  }

  @Test
  void ignoresCancellationPropagation() {
    NoopRequest request = newNoopRequest();
    Mono<NoopResponse> mono = Reactor.wrap(request, request.response(), false);

    assertThrows(Exception.class, () -> mono.timeout(Duration.ofMillis(10)).block());
    assertFalse(request.response().isCompletedExceptionally());
    assertFalse(request.response().isDone());
  }

  @Test
  void noErrorDroppedWhenCancelledViaCompletionException() {
    AtomicInteger droppedErrors = new AtomicInteger(0);
    Hooks.onErrorDropped(v -> droppedErrors.incrementAndGet());

    NoopRequest request = newNoopRequest();

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
    Hooks.onErrorDropped(v -> droppedErrors.incrementAndGet());

    NoopRequest request = newNoopRequest();

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
