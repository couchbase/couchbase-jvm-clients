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

package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CbThreadsTest {

  @Test
  void platformThreadIsNotVirtual() {
    assertFalse(CbThreads.isVirtual(Thread.currentThread()));
    assertFalse(CbThreads.isVirtual(CbThreads.platformThreadFactory("test-platform-").newThread(() -> {
    })));
  }

  @Test
  void virtualThreadIsVirtual() throws Exception {
    ThreadFactory virtualThreads = CbThreads.virtualThreadFactoryOrNull("test-virtual-");
    assumeTrue(virtualThreads != null, "needs virtual threads");

    AtomicBoolean virtual = new AtomicBoolean();
    Thread t = virtualThreads.newThread(() -> virtual.set(CbThreads.isVirtual(Thread.currentThread())));
    t.start();
    t.join();
    assertTrue(virtual.get());
  }
}
