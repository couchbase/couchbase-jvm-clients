/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the functionality of the {@link SingleStateful}.
 */
class SingleStatefulTest {

  @Test
  void loadsWithInitialState() {
    SingleStateful<Integer> stateful = SingleStateful.fromInitial(1);
    assertEquals(1, stateful.state());
    assertEquals(1, stateful.states().blockFirst());
  }

  @Test
  void failsOnNullInitialValue() {
    assertThrows(InvalidArgumentException.class, () -> SingleStateful.fromInitial(null));
  }

  @Test
  void failsOnNullTransitionValue() {
    SingleStateful<String> stateful = SingleStateful.fromInitial("abc");
    assertThrows(InvalidArgumentException.class, () -> stateful.transition(null));
    assertThrows(InvalidArgumentException.class, () -> stateful.compareAndTransition("abc", null));
  }

  @Test
  void pushesNewStates() {
    SingleStateful<Long> stateful = SingleStateful.fromInitial(1L);

    Flux
      .interval(Duration.ofMillis(200), Duration.ofMillis(100))
      .take(5)
      .subscribe(stateful::transition, e -> {}, stateful::close);

    List<Long> collectedStates = stateful.states().collectList().block();
    assertNotNull(collectedStates);
    assertEquals(6, collectedStates.size());
  }

  @Test
  void concurrentTransitionsAreEmittedInOrder() throws Exception {
    SingleStateful<TestState> stateful = SingleStateful.fromInitial(TestState.A);

    AtomicReference<TestState> lastEmitted = new AtomicReference<>();
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    stateful.states().subscribe(s -> {
      if (lastEmitted.getAndSet(s) == s) {
        errors.add(new AssertionError("Emitted same state twice in a row: " + s));
      }
    });

    int iterations = 20_000;
    CountDownLatch start = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();
    threads.add(new Thread(() -> {
      await(start);
      for (int i = 0; i < iterations; i++) {
        stateful.transition(TestState.A);
        stateful.compareAndTransition(TestState.A, TestState.B);
      }
    }));
    threads.add(new Thread(() -> {
      await(start);
      for (int i = 0; i < iterations; i++) {
        stateful.transition(TestState.C);
      }
    }));

    for (Thread t : threads) {
      t.setUncaughtExceptionHandler((thread, e) -> errors.add(e));
      t.start();
    }
    start.countDown();
    for (Thread t : threads) {
      t.join();
    }

    assertEquals(new ArrayList<>(), errors);
    assertEquals(stateful.state(), lastEmitted.get());
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  enum TestState {
    A,
    B,
    C
  }

}
