/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.retry.reactor;

import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

class RetryTest {

  private final Queue<RetryContext<?>> retries = new ConcurrentLinkedQueue<>();

  @Test
  public void shouldTimeoutRetryWithVirtualTime() {
    final int minBackoff = 1;
    final int maxBackoff = 5;
    final int timeout = 10;

    StepVerifier.withVirtualTime(() ->
      Mono.<String>error(new RuntimeException("Something went wrong"))
        .retryWhen(Retry.anyOf(Exception.class)
          .exponentialBackoffWithJitter(Duration.ofSeconds(minBackoff), Duration.ofSeconds(maxBackoff))
          .timeout(Duration.ofSeconds(timeout)).toReactorRetry())
        .subscribeOn(Schedulers.elastic()))
      .expectSubscription()
      .thenAwait(Duration.ofSeconds(timeout))
      .expectError(RetryExhaustedException.class)
      .verify(Duration.ofSeconds(timeout));
  }

  @Test
  void fluxRetryNoBackoff() {
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException()))
      .retryWhen(Retry.any().noBackoff().retryMax(2).doOnRetry(onRetry()).toReactorRetry());

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1)
      .verifyError(RetryExhaustedException.class);
    assertRetries(IOException.class, IOException.class);
    RetryTestUtils.assertDelays(retries, 0L, 0L);
  }

  @Test
  void monoRetryNoBackoff() {
    Mono<?> mono = Mono.error(new IOException())
      .retryWhen(Retry.any().noBackoff().retryMax(2).doOnRetry(onRetry()).toReactorRetry());

    StepVerifier.create(mono)
      .verifyError(RetryExhaustedException.class);
    assertRetries(IOException.class, IOException.class);
    RetryTestUtils.assertDelays(retries, 0L, 0L);
  }

  @Test
  void fluxRetryFixedBackoff() {
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException()))
      .retryWhen(Retry.any().fixedBackoff(Duration.ofMillis(500)).retryOnce().doOnRetry(onRetry()).toReactorRetry());

    StepVerifier.withVirtualTime(() -> flux)
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(300))
      .thenAwait(Duration.ofMillis(300))
      .expectNext(0, 1)
      .verifyError(RetryExhaustedException.class);
    assertRetries(IOException.class);
    RetryTestUtils.assertDelays(retries, 500L);
  }

  @Test
  void monoRetryFixedBackoff() {
    Mono<?> mono = Mono.error(new IOException())
      .retryWhen(Retry.any().fixedBackoff(Duration.ofMillis(500)).retryOnce().doOnRetry(onRetry()).toReactorRetry());

    StepVerifier.withVirtualTime(() -> mono)
      .expectSubscription()
      .expectNoEvent(Duration.ofMillis(300))
      .thenAwait(Duration.ofMillis(300))
      .verifyError(RetryExhaustedException.class);

    assertRetries(IOException.class);
    RetryTestUtils.assertDelays(retries, 500L);
  }


  @Test
  void fluxRetryExponentialBackoff() {
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException()))
      .retryWhen(Retry.any()
        .exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
        .timeout(Duration.ofMillis(1500))
        .doOnRetry(onRetry())
        .toReactorRetry());

    StepVerifier.create(flux)
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(50))  // delay=100
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(150)) // delay=200
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(250)) // delay=400
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(450)) // delay=500
      .expectNext(0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, IOException.class));

    assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
    RetryTestUtils.assertDelays(retries, 100L, 200L, 400L, 500L);
  }
  @Test
  void monoRetryExponentialBackoff() {
    Mono<?> mono = Mono.error(new IOException())
      .retryWhen(Retry.any()
        .exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
        .retryMax(4)
        .doOnRetry(onRetry())
        .toReactorRetry());

    StepVerifier.withVirtualTime(() -> mono)
      .expectSubscription()
      .thenAwait(Duration.ofMillis(100))
      .thenAwait(Duration.ofMillis(200))
      .thenAwait(Duration.ofMillis(400))
      .thenAwait(Duration.ofMillis(500))
      .verifyError(RetryExhaustedException.class);

    assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
    RetryTestUtils.assertDelays(retries, 100L, 200L, 400L, 500L);
  }

  @Test
  void fluxRetryRandomBackoff() {
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException()))
      .retryWhen(Retry.any()
        .randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
        .retryMax(4)
        .doOnRetry(onRetry())
        .toReactorRetry());

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1, 0, 1, 0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, IOException.class));

    assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
    RetryTestUtils.assertRandomDelays(retries, 100, 2000);
  }

  @Test
  void monoRetryRandomBackoff() {
    Mono<?> mono = Mono.error(new IOException())
      .retryWhen(Retry.any()
        .randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000))
        .retryMax(4)
        .doOnRetry(onRetry())
        .toReactorRetry());

    StepVerifier.withVirtualTime(() -> mono)
      .expectSubscription()
      .thenAwait(Duration.ofMillis(100))
      .thenAwait(Duration.ofMillis(2000))
      .thenAwait(Duration.ofMillis(2000))
      .thenAwait(Duration.ofMillis(2000))
      .verifyError(RetryExhaustedException.class);

    assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);
    RetryTestUtils.assertRandomDelays(retries, 100, 2000);
  }


  @Test
  void fluxRetriableExceptions() {
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new SocketException()))
      .retryWhen(Retry.anyOf(IOException.class).retryOnce().doOnRetry(onRetry()).toReactorRetry());

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

    Flux<Integer> nonRetriable = Flux.concat(Flux.range(0, 2), Flux.error(new RuntimeException()))
      .retryWhen(Retry.anyOf(IOException.class).retryOnce().doOnRetry(onRetry()).toReactorRetry());
    StepVerifier.create(nonRetriable)
      .expectNext(0, 1)
      .verifyError(RuntimeException.class);
  }

  @Test
  void fluxNonRetriableExceptions() {
    reactor.util.retry.Retry retry = Retry.allBut(RuntimeException.class).retryOnce().doOnRetry(onRetry()).toReactorRetry();
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IllegalStateException())).retryWhen(retry);

    StepVerifier.create(flux)
      .expectNext(0, 1)
      .verifyError(IllegalStateException.class);

    Flux<Integer> retriable = Flux.concat(Flux.range(0, 2), Flux.error(new SocketException())).retryWhen(retry);
    StepVerifier.create(retriable)
      .expectNext(0, 1, 0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));
  }

  @Test
  void fluxRetryAnyException() {
    reactor.util.retry.Retry retry = Retry.any().retryOnce().doOnRetry(onRetry()).toReactorRetry();

    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new SocketException())).retryWhen(retry);
    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

    Flux<Integer> flux2 = Flux.concat(Flux.range(0, 2), Flux.error(new RuntimeException())).retryWhen(retry);
    StepVerifier.create(flux2)
      .expectNext(0, 1, 0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, RuntimeException.class));
  }

  @Test
  void fluxRetryOnPredicate() {
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new SocketException()))
      .retryWhen(Retry.onlyIf(context -> context.iteration() < 3).doOnRetry(onRetry()).toReactorRetry());

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1)
      .verifyError(SocketException.class);
  }

  @Test
  void doOnRetry() {
    Semaphore semaphore = new Semaphore(0);
    reactor.util.retry.Retry retry = Retry.any()
      .retryOnce()
      .fixedBackoff(Duration.ofMillis(500))
      .doOnRetry(context -> semaphore.release())
      .toReactorRetry();

    StepVerifier.withVirtualTime(() -> Flux.range(0, 2).concatWith(Mono.error(new SocketException())).retryWhen(retry))
      .expectNext(0, 1)
      .then(semaphore::acquireUninterruptibly)
      .expectNoEvent(Duration.ofMillis(400))
      .thenAwait(Duration.ofMillis(200))
      .expectNext(0, 1)
      .verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));

    StepVerifier.withVirtualTime(() -> Mono.error(new SocketException()).retryWhen(Retry.any()
      .retryOnce()
      .fixedBackoff(Duration.ofMillis(500))
      .doOnRetry(context -> semaphore.release()).noBackoff().toReactorRetry()))
      .then(semaphore::acquireUninterruptibly)
      .verifyErrorMatches(e -> isRetryExhausted(e, SocketException.class));
  }

  @Test
  void retryApplicationContext() {
    class AppContext {
      boolean needsRollback;
      void rollback() {
        needsRollback = false;
      }
      void run() {
        assertFalse("Rollback not performed", needsRollback);
        needsRollback = true;
      }
    }
    AppContext appContext = new AppContext();
    reactor.util.retry.Retry retry = Retry.<AppContext>any().withApplicationContext(appContext)
      .retryMax(2)
      .doOnRetry(context -> {
        AppContext ac = context.applicationContext();
        assertNotNull("Application context not propagated", ac);
        ac.rollback();
      })
      .toReactorRetry();

    StepVerifier.withVirtualTime(() -> Mono.error(new RuntimeException()).doOnNext(i -> appContext.run()).retryWhen(retry))
      .verifyErrorMatches(e -> isRetryExhausted(e, RuntimeException.class));

  }

  @Test
  void fluxRetryCompose() {
    Retry<?> retry = Retry.any().noBackoff().retryMax(2).doOnRetry(this.onRetry());
    Flux<Integer> flux = Flux.concat(Flux.range(0, 2), Flux.error(new IOException())).as(retry::apply);

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1)
      .verifyError(RetryExhaustedException.class);
    assertRetries(IOException.class, IOException.class);
  }

  @Test
  void monoRetryCompose() {
    Retry<?> retry = Retry.any().noBackoff().retryMax(2).doOnRetry(this.onRetry());
    Flux<?> flux = Mono.error(new IOException()).as(retry::apply);

    StepVerifier.create(flux)
      .verifyError(RetryExhaustedException.class);
    assertRetries(IOException.class, IOException.class);
  }

  Consumer<? super RetryContext<?>> onRetry() {
    return retries::add;
  }

  @SafeVarargs
  private final void assertRetries(Class<? extends Throwable>... exceptions) {
    assertEquals(exceptions.length, retries.size());
    int index = 0;
    for (RetryContext<?> retryContext : retries) {
      assertEquals(index + 1, retryContext.iteration());
      assertEquals(exceptions[index], retryContext.exception().getClass());
      index++;
    }
  }

  static boolean isRetryExhausted(Throwable e, Class<? extends Throwable> cause) {
    return e instanceof RetryExhaustedException && cause.isInstance(e.getCause());
  }

  @Test
  void retryToString() {
    assertEquals(
      "Retry{max=2,backoff=Backoff{ZERO},jitter=Jitter{NONE}}",
      Retry.any().noBackoff().retryMax(2).toString()
    );
  }

}
