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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

  /**
   * Some test states to assert against.
   */
  enum SomeStates {
    DISCONNECTED,
    DEGRADED,
    CONNECTED
  }

}