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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the functionality of the {@link SingleStateful}.
 */
class SingleStatefulTest {

  @Test
  void loadsWithInitialState() {
    SingleStateful<Integer> stateful = SingleStateful.fromInitial(1);
    assertEquals(1, stateful.state());
  }

  @Test
  void failsOnNullInitialValue() {
    assertThrows(IllegalArgumentException.class, () -> SingleStateful.fromInitial(null));
  }

  @Test
  void failsOnNullTransitionValue() {
    SingleStateful<String> stateful = SingleStateful.fromInitial("abc");
    assertThrows(IllegalArgumentException.class, () -> stateful.transition(null));
    assertThrows(IllegalArgumentException.class, () -> stateful.compareAndTransition("abc", null));
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
    assertEquals(5, collectedStates.size());
  }

}