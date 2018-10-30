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
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.msg.kv.NoopResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Duration;

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
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, true);

    NoopResponse response = mock(NoopResponse.class);
    StepVerifier verifier = StepVerifier.create(mono).expectNext(response).expectComplete();

    request.succeed(response);
    verifier.verify();
  }

  @Test
  void completesWithErrorAfterSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, true);

    RequestCanceledException exception = mock(RequestCanceledException.class);
    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);

    request.fail(exception);
    verifier.verify();
  }

  @Test
  void completesWithSuccessBeforeSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class));
    NoopResponse response = mock(NoopResponse.class);
    request.succeed(response);
    Mono<NoopResponse> mono = Reactor.wrap(request, true);

    StepVerifier verifier = StepVerifier.create(mono).expectNext(response).expectComplete();
    verifier.verify();
  }

  @Test
  void completesWithErrorBeforeSubscription() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class));
    RequestCanceledException exception = mock(RequestCanceledException.class);
    request.fail(exception);
    Mono<NoopResponse> mono = Reactor.wrap(request, true);

    StepVerifier verifier = StepVerifier.create(mono).expectError(RequestCanceledException.class);
    verifier.verify();
  }

  @Test
  void propagatesCancellation() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, true);

    assertThrows(Exception.class, () -> mono.timeout(Duration.ofMillis(10)).block());
    assertTrue(request.response().isCompletedExceptionally());
    assertTrue(request.response().isDone());
  }

  @Test
  void ignoresCancellationPropagation() {
    NoopRequest request = new NoopRequest(Duration.ZERO, mock(RequestContext.class));
    Mono<NoopResponse> mono = Reactor.wrap(request, false);

    assertThrows(Exception.class, () -> mono.timeout(Duration.ofMillis(10)).block());
    assertFalse(request.response().isCompletedExceptionally());
    assertFalse(request.response().isDone());
  }

}