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

package com.couchbase.client.java;

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class AutoDisconnectIntegrationTest extends JavaIntegrationTest {
  @Test
  void clusterAutomaticallyDisconnected() {
    SimpleEventBus eventBus = new SimpleEventBus(true);
    createCluster(env -> env.eventBus(eventBus));
    collectGarbageAndAssert(() -> clusterLeakDetected(eventBus));
  }

  @Test
  void reachableBucketPreventsAutoDisconnect() {
    SimpleEventBus eventBus = new SimpleEventBus(true);

    @SuppressWarnings("WriteOnlyObject")
    AtomicReference<Bucket> bucketRef = new AtomicReference<>(
      createCluster(env -> env.eventBus(eventBus))
        .bucket(config().bucketname())
    );

    Deadline deadline = Deadline.of(Duration.ofSeconds(3));
    collectGarbageAndAssert(() -> {
      assertFalse(clusterLeakDetected(eventBus), "Cluster was auto-disconnected even though Bucket is reachable.");
      return deadline.exceeded(); // succeed if no leak is detected before deadline
    });

    bucketRef.set(null);

    // now the cluster should be auto-closed
    collectGarbageAndAssert(() -> clusterLeakDetected(eventBus));
  }

  private static boolean clusterLeakDetected(SimpleEventBus eventBus) {
    return eventBus.publishedEvents().stream()
      .anyMatch(it -> it.getClass().getSimpleName().equals("ClusterLeakDetected"));
  }

  private static void collectGarbageAndAssert(Supplier<Boolean> condition) {
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
