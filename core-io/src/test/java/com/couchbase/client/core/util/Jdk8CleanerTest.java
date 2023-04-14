/*
 * Copyright 2023 Couchbase, Inc.
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

import com.couchbase.client.core.env.CouchbaseThreadFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.fail;

class Jdk8CleanerTest {
  @Test
  void register() throws Exception {
    Jdk8Cleaner cleaner = Jdk8Cleaner.create(new CouchbaseThreadFactory("jdk8cleaner"));

    AtomicBoolean cleaned = new AtomicBoolean();
    // Lambda is only safe here because it doesn't capture the value of the referent.
    // Normally you'd pass an instance of a static class that implements Runnable.
    cleaner.register(new Object(), () -> cleaned.set(true));

    collectGarbageAndAssert(cleaned::get);
  }

  @Test
  void oneShotCleanerThreadTerminatesAfterCleanup() throws Exception {
    AtomicBoolean cleaned = new AtomicBoolean();
    AtomicReference<Object> doNotGarbageCollect = new AtomicReference<>(new Object());

    // Registering a new one-shot cleaner should increment the number of active cleaner threads
    long prevCount = Jdk8Cleaner.activeCleanerThreadCount.get();
    Jdk8Cleaner.registerWithOneShotCleaner(doNotGarbageCollect.get(), () -> cleaned.set(true));
    waitUntilCondition(() -> Jdk8Cleaner.activeCleanerThreadCount.get() == prevCount + 1);

    // Completing the cleaner task should decrement the number of active cleaner threads
    doNotGarbageCollect.set(null);
    collectGarbageAndAssert(() -> cleaned.get() && Jdk8Cleaner.activeCleanerThreadCount.get() == prevCount);
  }

  public static void collectGarbageAndAssert(Supplier<Boolean> condition) {
    Deadline deadline = Deadline.of(Duration.ofSeconds(15));

    do {
      System.gc();
      if (condition.get()) {
        return;
      }
    } while (!deadline.exceeded());

    fail("Failed to meet condition before deadline.");
  }
}
