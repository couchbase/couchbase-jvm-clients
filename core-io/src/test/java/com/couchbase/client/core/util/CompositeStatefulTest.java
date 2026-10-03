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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of composing different stateful components together.
 */
class CompositeStatefulTest {

  @Test
  void initializedWithInitialState() {
    CompositeStateful<String, SomeStates, SomeStates> composite = CompositeStateful.create(
      SomeStates.DISCONNECTED,
      states ->  SomeStates.DISCONNECTED
    );
    assertEquals(SomeStates.DISCONNECTED, composite.state());
  }

  @Test
  void canRegisterAndDeregister() {
    CompositeStateful<String, SomeStates, SomeStates> composite = CompositeStateful.create(
      SomeStates.DISCONNECTED,
      states -> {
        int connected = 0;
        for (SomeStates state : states) {
          if (state == SomeStates.CONNECTED) {
            connected++;
          }
        }
        if (states.size() == connected) {
          return SomeStates.CONNECTED;
        } else if (connected > 0) {
          return SomeStates.DEGRADED;
        } else {
          return SomeStates.DISCONNECTED;
        }
      }
    );

    List<SomeStates> emittedStates = Collections.synchronizedList(new ArrayList<>());
    composite.states().subscribe(emittedStates::add);

    assertEquals(SomeStates.DISCONNECTED, composite.state());

    SingleStateful<SomeStates> node1 = SingleStateful.fromInitial(SomeStates.CONNECTED);
    composite.register("node1", node1);
    assertEquals(SomeStates.CONNECTED, composite.state());

    SingleStateful<SomeStates> node2 = SingleStateful.fromInitial(SomeStates.DISCONNECTED);
    composite.register("node2", node2);
    assertEquals(SomeStates.DEGRADED, composite.state());

    node2.transition(SomeStates.CONNECTED);
    assertEquals(SomeStates.CONNECTED, composite.state());

    node1.transition(SomeStates.DISCONNECTED);
    assertEquals(SomeStates.DEGRADED, composite.state());

    composite.deregister("node1");
    assertEquals(SomeStates.CONNECTED, composite.state());

    composite.deregister("node2");
    assertEquals(SomeStates.DISCONNECTED, composite.state());

    assertTrue(emittedStates.size() > 0);
  }

  @Test
  void elementIsRemovedWhenItsStateStreamCompletes() {
    CompositeStateful<String, SomeStates, SomeStates> composite = newComposite();

    SingleStateful<SomeStates> node1 = SingleStateful.fromInitial(SomeStates.CONNECTED);
    SingleStateful<SomeStates> node2 = SingleStateful.fromInitial(SomeStates.DISCONNECTED);
    composite.register("node1", node1);
    composite.register("node2", node2);
    assertEquals(SomeStates.DEGRADED, composite.state());

    node2.close();
    assertEquals(SomeStates.CONNECTED, composite.state());

    // Already removed; must not affect the other element.
    composite.deregister("node2");
    assertEquals(SomeStates.CONNECTED, composite.state());

    node1.close();
    assertEquals(SomeStates.DISCONNECTED, composite.state());
  }

  @Test
  void signalsFromReplacedRegistrationAreIgnored() {
    CompositeStateful<String, SomeStates, SomeStates> composite = newComposite();

    SingleStateful<SomeStates> original = SingleStateful.fromInitial(SomeStates.CONNECTED);
    SingleStateful<SomeStates> replacement = SingleStateful.fromInitial(SomeStates.DISCONNECTED);
    composite.register("node", original);
    composite.register("node", replacement);
    assertEquals(SomeStates.DISCONNECTED, composite.state());

    original.transition(SomeStates.DISCONNECTED);
    original.transition(SomeStates.CONNECTED);
    original.close();
    assertEquals(SomeStates.DISCONNECTED, composite.state());

    replacement.transition(SomeStates.CONNECTED);
    assertEquals(SomeStates.CONNECTED, composite.state());
  }

  @Test
  void closeTransitionsToInitialStateAndCompletesStream() {
    CompositeStateful<String, SomeStates, SomeStates> composite = newComposite();

    SingleStateful<SomeStates> node = SingleStateful.fromInitial(SomeStates.CONNECTED);
    composite.register("node", node);
    assertEquals(SomeStates.CONNECTED, composite.state());

    composite.close();
    assertEquals(SomeStates.DISCONNECTED, composite.state());
    assertEquals(
      Collections.singletonList(SomeStates.DISCONNECTED),
      composite.states().collectList().block(Duration.ofSeconds(5))
    );

    // Elements are no longer tracked, and new ones are ignored.
    node.transition(SomeStates.DEGRADED);
    composite.register("other", SingleStateful.fromInitial(SomeStates.CONNECTED));
    assertEquals(SomeStates.DISCONNECTED, composite.state());
  }

  @Test
  void concurrentElementChangesAreNotLost() throws Exception {
    SingleStateful<SomeStates> node1 = SingleStateful.fromInitial(SomeStates.DISCONNECTED);
    SingleStateful<SomeStates> node2 = SingleStateful.fromInitial(SomeStates.DISCONNECTED);
    Thread other = new Thread(() -> node2.transition(SomeStates.CONNECTED));

    // After computing the composite state for node1's change (but before applying it),
    // let node2 change too. Without proper locking, node2's change is applied first,
    // then overwritten by the stale state computed for node1's change.
    AtomicBoolean pauseNextAggregation = new AtomicBoolean();
    CompositeStateful<String, SomeStates, SomeStates> composite = CompositeStateful.create(
      SomeStates.DISCONNECTED,
      states -> {
        SomeStates result = aggregate(states);
        if (pauseNextAggregation.compareAndSet(true, false)) {
          SingleStatefulTest.startAndAwaitDoneOrBlocked(other);
        }
        return result;
      }
    );
    composite.register("node1", node1);
    composite.register("node2", node2);

    pauseNextAggregation.set(true);
    node1.transition(SomeStates.CONNECTED);
    other.join();

    assertEquals(SomeStates.CONNECTED, composite.state());
  }

  private static CompositeStateful<String, SomeStates, SomeStates> newComposite() {
    return CompositeStateful.create(SomeStates.DISCONNECTED, CompositeStatefulTest::aggregate);
  }

  private static SomeStates aggregate(Collection<SomeStates> states) {
    int connected = 0;
    for (SomeStates state : states) {
      if (state == SomeStates.CONNECTED) {
        connected++;
      }
    }
    if (states.isEmpty()) {
      return SomeStates.DISCONNECTED;
    } else if (states.size() == connected) {
      return SomeStates.CONNECTED;
    } else if (connected > 0) {
      return SomeStates.DEGRADED;
    } else {
      return SomeStates.DISCONNECTED;
    }
  }

  /**
   * Some test states to assert against.
   */
  enum SomeStates {
    DISCONNECTED,
    DEGRADED,
    CONNECTED
  }

}
