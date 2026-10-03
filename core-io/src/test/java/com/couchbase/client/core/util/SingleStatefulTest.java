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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the {@link SingleStateful}.
 */
class SingleStatefulTest {

    /**
   * Starts a thread and waits until it either finishes or blocks waiting for a monitor.
   * <p>
   * Lets a test pause one thread inside a critical section and run another thread "inside" it:
   * if the critical section is properly guarded, the other thread blocks; otherwise it runs to completion.
   */
  static void startAndAwaitDoneOrBlocked(Thread thread) {
    thread.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Thread neither finished nor blocked: " + thread.getState());
      }
      Thread.yield();
    }
  }

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
    AtomicReference<SingleStateful<TestState>> ref = new AtomicReference<>();
    Thread other = new Thread(() -> ref.get().transition(TestState.C));

    // After compareAndTransition changes the state (but before it emits the new state),
    // let another thread transition too. Without proper locking, the other thread's state
    // is emitted first, so subscribers end up seeing a state that is no longer current.
    AtomicBoolean pauseNextTransition = new AtomicBoolean();
    SingleStateful<TestState> stateful = SingleStateful.fromInitial(TestState.A, (oldState, newState) -> {
      if (pauseNextTransition.compareAndSet(true, false)) {
        startAndAwaitDoneOrBlocked(other);
      }
    });
    ref.set(stateful);

    List<TestState> emitted = new CopyOnWriteArrayList<>();
    stateful.states().subscribe(emitted::add);

    pauseNextTransition.set(true);
    assertTrue(stateful.compareAndTransition(TestState.A, TestState.B));
    other.join();

    assertEquals(TestState.C, stateful.state());
    assertEquals(Arrays.asList(TestState.A, TestState.B, TestState.C), emitted);
  }

  enum TestState {
    A,
    B,
    C
  }

}
