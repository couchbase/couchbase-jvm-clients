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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

class RepeatTest {

  private ConcurrentLinkedQueue<RepeatContext<?>> repeats = new ConcurrentLinkedQueue<>();

  @Test
  void fluxRepeatNoBackoff() {
    Flux<Integer> flux = Flux.range(0, 2).repeatWhen(Repeat.times(3).noBackoff().doOnRepeat(onRepeat()));

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1, 0, 1)
      .verifyComplete();
    assertRepeats(2L, 2L, 2L);
    RetryTestUtils.assertDelays(repeats, 0L, 0L, 0L);
  }

  @Test
  void monoRepeatNoBackoff() {
    Repeat<?> repeat = Repeat.times(3).noBackoff().doOnRepeat(onRepeat());
    Flux<Integer> flux =  Mono.just(0).repeatWhen(repeat);

    StepVerifier.create(flux)
      .expectNext(0, 0, 0, 0)
      .verifyComplete();
    assertRepeats(1L, 1L, 1L);
    RetryTestUtils.assertDelays(repeats, 0L, 0L, 0L);

    // Test with empty Mono
    repeats.clear();
    StepVerifier.create(Mono.empty().repeatWhen(repeat))
      .verifyComplete();
    assertRepeats(0L, 0L, 0L);
    RetryTestUtils.assertDelays(repeats, 0L, 0L, 0L);
  }

  @Test
  void monoRepeatEmptyNoBackoff() {
    Repeat<?> repeat = Repeat.times(3).noBackoff().doOnRepeat(onRepeat());
    Mono<Integer> mono = Mono.<Integer>empty().repeatWhenEmpty(repeat);

    StepVerifier.create(mono)
      .verifyComplete();
    assertRepeats(0L, 1L, 2L);
    RetryTestUtils.assertDelays(repeats, 0L, 0L, 0L);
  }


  @Test
  void fluxRepeatFixedBackoff() {
    Repeat<?> repeat = Repeat.once().fixedBackoff(Duration.ofMillis(500)).doOnRepeat(onRepeat());
    Flux<Integer> flux = Flux.range(0, 2).repeatWhen(repeat);

    StepVerifier.withVirtualTime(() -> flux)
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(300))
      .thenAwait(Duration.ofMillis(300))
      .expectNext(0, 1)
      .verifyComplete();
    assertRepeats(2L);
    RetryTestUtils.assertDelays(repeats, 500L);
  }

  @Test
  void monoRepeatFixedBackoff() {
    Repeat<?> repeat = Repeat.once().fixedBackoff(Duration.ofMillis(500)).doOnRepeat(onRepeat());
    Flux<Integer> flux = Mono.just(0).repeatWhen(repeat);

    StepVerifier.withVirtualTime(() ->flux)
      .expectNext(0)
      .expectNoEvent(Duration.ofMillis(300))
      .thenAwait(Duration.ofMillis(300))
      .expectNext(0)
      .verifyComplete();
    assertRepeats(1L);
    RetryTestUtils.assertDelays(repeats, 500L);
  }

  @Test
  void monoRepeatEmptyFixedBackoff() {
    Repeat<?> repeat = Repeat.once().fixedBackoff(Duration.ofMillis(500)).doOnRepeat(onRepeat());
    Mono<Integer> mono = Mono.<Integer>empty().repeatWhenEmpty(repeat);

    StepVerifier.withVirtualTime(() -> mono)
      .expectSubscription()
      .expectNoEvent(Duration.ofMillis(300))
      .thenAwait(Duration.ofMillis(300))
      .verifyComplete();
    assertRepeats(0L);
    RetryTestUtils.assertDelays(repeats, 500L);
  }

  @Test
  void fluxRepeatExponentialBackoff() {
    Repeat<?> repeat = Repeat.times(4).exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500)).doOnRepeat(onRepeat());
    Flux<Integer> flux = Flux.range(0, 2).repeatWhen(repeat);

    StepVerifier.withVirtualTime(() -> flux)
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(50))  // delay=100
      .thenAwait(Duration.ofMillis(100))
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(150)) // delay=200
      .thenAwait(Duration.ofMillis(100))
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(250)) // delay=400
      .thenAwait(Duration.ofMillis(100))
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(450)) // delay=500
      .thenAwait(Duration.ofMillis(100))
      .expectNext(0, 1)
      .verifyComplete();

    assertRepeats(2L, 2L, 2L, 2L);
    RetryTestUtils.assertDelays(repeats, 100L, 200L, 400L, 500L);
  }

  @Test
  void monoRepeatExponentialBackoff() {
    Repeat<?> repeat = Repeat.times(4).exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500)).doOnRepeat(onRepeat());
    Flux<Integer> flux = Mono.just(0).repeatWhen(repeat);

    StepVerifier.withVirtualTime(() -> flux)
      .expectNext(0)
      .thenAwait(Duration.ofMillis(100))
      .expectNext(0)
      .thenAwait(Duration.ofMillis(200))
      .expectNext(0)
      .thenAwait(Duration.ofMillis(400))
      .expectNext(0)
      .thenAwait(Duration.ofMillis(500))
      .expectNext(0)
      .verifyComplete();
    assertRepeats(1L, 1L, 1L, 1L);
    RetryTestUtils.assertDelays(repeats, 100L, 200L, 400L, 500L);
  }

  @Test
  void monoRepeatEmptyExponentialBackoff() {
    Repeat<?> repeat = Repeat.times(4).exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500)).doOnRepeat(onRepeat());
    Mono<Integer> mono = Mono.<Integer>empty().repeatWhenEmpty(repeat);

    StepVerifier.withVirtualTime(() -> mono)
      .expectSubscription()
      .thenAwait(Duration.ofMillis(100))
      .thenAwait(Duration.ofMillis(200))
      .thenAwait(Duration.ofMillis(400))
      .thenAwait(Duration.ofMillis(500))
      .verifyComplete();

    assertRepeats(0L, 1L, 2L, 3L);
    RetryTestUtils.assertDelays(repeats, 100L, 200L, 400L, 500L);
  }

  @Test
  void fluxRepeatRandomBackoff() {
    Repeat<?> repeat = Repeat.times(4)
      .randomBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
      .doOnRepeat(onRepeat());

    StepVerifier.withVirtualTime(() -> Flux.range(0, 2)
      .repeatWhen(repeat)
      .elapsed()
      .filter(t2 -> t2.getT2() == 0)
      .map(Tuple2::getT1))
      .recordWith(ArrayList::new)
      .expectNoEvent(Duration.ofMillis(100)) //even with jitter, can't go under min
      .thenAwait(Duration.ofHours(2)) //quickly go to end of sequence
      .expectNextCount(5)
      .consumeRecordedWith(l -> assertThat(l).hasSize(5)
        .startsWith(0L)
        .containsAll(repeats
          .stream()
          .map(c -> c.backoff().toMillis())
          .collect(Collectors.toList())
        ))
      .verifyComplete();

    assertRepeats(2L, 2L, 2L, 2L);
    RetryTestUtils.assertRandomDelays(repeats, 100, 500);
  }


  @Test
  void monoRepeatRandomBackoff() {
    Repeat<?> repeat = Repeat.times(4).randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000)).doOnRepeat(onRepeat());

    StepVerifier.withVirtualTime(() -> Mono.just(0)
      .repeatWhen(repeat)
      .elapsed()
      .map(Tuple2::getT1))
      .recordWith(ArrayList::new)
      .expectNoEvent(Duration.ofMillis(100)) //even with jitter, can't go under min
      .thenAwait(Duration.ofHours(2)) //quickly go to end of sequence
      .expectNextCount(5)
      .consumeRecordedWith(l -> assertThat(l).hasSize(5)
        .startsWith(0L)
        .containsAll(repeats
          .stream()
          .map(c -> c.backoff().toMillis())
          .collect(Collectors.toList())
        ))
      .verifyComplete();
    assertRepeats(1L, 1L, 1L, 1L);
    RetryTestUtils.assertRandomDelays(repeats, 100, 2000);
  }

  @Test
  void monoRepeatEmptyRandomBackoff() {
    Repeat<?> repeat = Repeat.times(4).randomBackoff(Duration.ofMillis(100), Duration.ofMillis(2000)).doOnRepeat(onRepeat());
    Mono<Integer> mono = Mono.<Integer>empty().repeatWhenEmpty(repeat);

    StepVerifier.withVirtualTime(() -> mono)
      .expectSubscription()
      .thenAwait(Duration.ofMillis(100))
      .thenAwait(Duration.ofMillis(2000))
      .thenAwait(Duration.ofMillis(2000))
      .thenAwait(Duration.ofMillis(2000))
      .verifyComplete();

    assertRepeats(0L, 1L, 2L, 3L);
    RetryTestUtils.assertRandomDelays(repeats, 100, 2000);
  }

  @Test
  void fluxRepeatOnPredicate() {
    Repeat<?> repeat = Repeat.onlyIf(context -> context.iteration() < 3).doOnRepeat(onRepeat());
    Flux<Integer> flux = Flux.range(0, 2).repeatWhen(repeat);

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1)
      .verifyComplete();
  }

  @Test
  void fluxRepeatCreate() {
    Repeat<?> repeat = Repeat.create(context -> context.iteration() < 3, 4).doOnRepeat(onRepeat());
    Flux<Integer> flux = Flux.range(0, 2).repeatWhen(repeat);

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1)
      .verifyComplete();
  }

  @Test
  void once() {
    Flux<Integer> flux = Flux.range(0, 2).repeatWhen(Repeat.once().doOnRepeat(onRepeat()));

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1)
      .verifyComplete();
  }

  @Test
  void backoffScheduler() {
    Scheduler backoffScheduler = Schedulers.newSingle("test");
    Repeat<?> repeat = Repeat.once().doOnRepeat(onRepeat())
      .fixedBackoff(Duration.ofMillis(100))
      .withBackoffScheduler(backoffScheduler);

    Semaphore semaphore = new Semaphore(0);
    backoffScheduler.schedule(semaphore::acquireUninterruptibly);
    StepVerifier.withVirtualTime(() -> Flux.range(0, 2).repeatWhen(repeat))
      .expectNext(0, 1)
      .expectNoEvent(Duration.ofMillis(200))
      .then(semaphore::release)
      .expectNext(0, 1)
      .verifyComplete();

    backoffScheduler.schedule(semaphore::acquireUninterruptibly);
    StepVerifier.withVirtualTime(() -> Mono.just(0).repeatWhen(repeat))
      .expectNext(0)
      .expectNoEvent(Duration.ofMillis(200))
      .then(semaphore::release)
      .expectNext(0)
      .verifyComplete();
  }

  @Test
  void doOnRepeat() {
    Semaphore semaphore = new Semaphore(0);
    Repeat<?> repeat = Repeat.once()
      .fixedBackoff(Duration.ofMillis(500))
      .doOnRepeat(context -> semaphore.release());

    StepVerifier.withVirtualTime(() -> Flux.range(0, 2).repeatWhen(repeat))
      .expectNext(0, 1)
      .then(semaphore::acquireUninterruptibly)
      .expectNoEvent(Duration.ofMillis(400))
      .thenAwait(Duration.ofMillis(200))
      .expectNext(0, 1)
      .verifyComplete();

    StepVerifier.withVirtualTime(() -> Mono.just(0).repeatWhen(repeat))
      .expectNext(0)
      .then(semaphore::acquireUninterruptibly)
      .expectNoEvent(Duration.ofMillis(400))
      .thenAwait(Duration.ofMillis(200))
      .expectNext(0)
      .verifyComplete();
  }

  @Test
  void repeatApplicationContext() {
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
    Repeat<?> repeat = Repeat.<AppContext>times(2)
      .withApplicationContext(appContext)
      .doOnRepeat(context -> {
        AppContext ac = context.applicationContext();
        assertNotNull("Application context not propagated", ac);
        ac.rollback();
      });

    StepVerifier.withVirtualTime(() -> Mono.just(1).doOnNext(i -> appContext.run()).repeatWhen(repeat))
      .expectNext(1, 1, 1)
      .verifyComplete();

  }

  @Test
  void fluxRepeatCompose() {
    Repeat<?> repeat = Repeat.times(2).noBackoff().doOnRepeat(onRepeat());
    Flux<Integer> flux = Flux.range(0, 2).as(repeat::apply);

    StepVerifier.create(flux)
      .expectNext(0, 1, 0, 1, 0, 1)
      .verifyComplete();
    assertRepeats(2L, 2L);
  }

  @Test
  void monoRepeatCompose() {
    Repeat<?> repeat = Repeat.times(3).noBackoff().doOnRepeat(onRepeat());
    Flux<Integer> flux = Mono.just(5).as(repeat::apply);

    StepVerifier.create(flux)
      .expectNext(5, 5, 5, 5)
      .verifyComplete();
    assertRepeats(1L, 1L, 1L);

    // Test with empty Mono
    repeats.clear();
    StepVerifier.create(Mono.empty().as(repeat::apply))
      .verifyComplete();
    assertRepeats(0L, 0L, 0L);
  }

  @Test
  void functionReuseInParallel() throws Exception {
    int repeatCount = 19;
    int range = 100;
    Integer[] values = new Integer[(repeatCount + 1) * range];
    for (int i = 0; i <= repeatCount; i++) {
      for (int j = 1; j <= range; j++)
        values[i * range + j - 1] = j;
    }
    RetryTestUtils.testReuseInParallel(2, 20,
      backoff -> Repeat.<Integer>times(19).backoff(backoff),
      repeatFunc -> StepVerifier.create(Flux.range(1, range).repeatWhen(repeatFunc))
          .expectNext(values)
          .verifyComplete());
  }

  Consumer<? super RepeatContext<?>> onRepeat() {
    return context -> repeats.add(context);
  }

  private void assertRepeats(Long... values) {
    assertEquals(values.length, repeats.size());
    int index = 0;
    for (RepeatContext<?> repeatContext : repeats) {
      assertEquals(index + 1, repeatContext.iteration());
      assertEquals(values[index], repeatContext.companionValue());
      index++;
    }
  }

}
