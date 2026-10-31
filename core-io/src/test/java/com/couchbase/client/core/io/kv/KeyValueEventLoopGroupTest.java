/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.kv;

import com.couchbase.client.core.env.CoreEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueEventLoopGroupTest {

  private static Set<String> loopThreadNames(String prefix) {
    return Thread.getAllStackTraces().keySet().stream()
      .filter(Thread::isAlive)
      .map(Thread::getName)
      .filter(name -> name.startsWith(prefix))
      .collect(Collectors.toSet());
  }

  @Test
  void environmentCreatesLoopsWhenFirstAskedSizedByEventLoopThreadCountAndShutsThemDown() {
    CoreEnvironment env = CoreEnvironment.builder()
      .ioEnvironment(io -> io.eventLoopThreadCount(3))
      .build();
    KeyValueEventLoopGroup group;
    String prefix;
    try {
      group = env.ioEnvironment().nioKeyValueEventLoopGroup();
      assertSame(group, env.ioEnvironment().nioKeyValueEventLoopGroup());
      assertEquals(3, group.threadCount());

      prefix = group.next().toString().replaceAll(".*\\{(cb-kv-nio-\\d+-)\\d+}", "$1");
      assertEquals(3, loopThreadNames(prefix).size(), prefix);
    } finally {
      env.shutdown(Duration.ofSeconds(10)); // once: shutting down an environment twice fails
    }

    assertTrue(loopThreadNames(prefix).isEmpty(), "threads still running: " + loopThreadNames(prefix));
    assertThrows(IllegalStateException.class, group::next);
  }

  @Test
  void environmentThatIsShutDownDoesNotCreateLoops() {
    CoreEnvironment env = CoreEnvironment.create();
    env.shutdown(Duration.ofSeconds(10));
    assertThrows(IllegalStateException.class, () -> env.ioEnvironment().nioKeyValueEventLoopGroup());
  }

  @Test
  void loopRunsTasksSubmittedAfterItStopped() throws Exception {
    KeyValueEventLoopGroup group = new KeyValueEventLoopGroup(1);
    KeyValueEventLoop loop = group.next();
    group.shutdown(Duration.ofSeconds(10)).block();

    boolean[] ran = new boolean[1];
    loop.execute(() -> ran[0] = true);
    assertTrue(ran[0], "a task submitted after the loop stopped runs on the submitting thread");
  }
}
