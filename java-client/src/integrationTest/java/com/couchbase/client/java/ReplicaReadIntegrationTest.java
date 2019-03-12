/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.cnc.events.request.IndividualReplicaGetFailedEvent;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This integration test verifies all different ways a replica read can be used.
 *
 * <p>Note that since naturally this depends on different topologies to be present (or not), you'll
 * find many annotations on the tests and the suite needs to be executed against different topologies
 * in order to execute them all.</p>
 */
class ReplicaReadIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterAll
  static void tearDown() {
    environment.shutdown();
    cluster.shutdown();
  }

  /**
   * As a simple litmus test, when ALL is used there should be at least one record coming back,
   * namely from the active.
   *
   * <p>Depending on the topology, it might be more than that.</p>
   */
  @Test
  void alwaysPassesWithAll() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, "Hello, World!");

    List<GetResult> results = collection.getFromReplica(id).collect(Collectors.toList());
    assertFalse(results.isEmpty());
    for (GetResult result : results) {
      assertEquals("Hello, World!", result.contentAs(String.class));
      assertFalse(result.expiration().isPresent());
    }
  }

  /**
   * This test only executes when there are at least two nodes and exactly one replica
   * defined.
   *
   * <p>Note that this is supposed to work on the mock, but for some reason it does fail depending
   * on the key chosen. once https://github.com/couchbase/CouchbaseMock/issues/47 is cleared up
   * the restriction can be lifted.</p>
   */
  @Test
  @IgnoreWhen(
    nodesLessThan = 2,
    replicasLessThan = 1,
    replicasGreaterThan = 1,
    clusterTypes = { ClusterType.MOCKED }
  )
  void twoResultsWithOneReplica() throws Exception {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "Hello, World!");

    // TODO: perform a "replicate to 1" instead
    Thread.sleep(1000);

    List<GetResult> results = collection.getFromReplica(id).collect(Collectors.toList());
    assertEquals(2, results.size());
    assertNotNull(results.get(0));
    assertNotNull(results.get(1));
    assertEquals("Hello, World!", results.get(0).contentAs(String.class));
    assertEquals("Hello, World!", results.get(1).contentAs(String.class));
  }

  /**
   * If we have a constellation where there are more replicas defined than available, a
   * subset of the requests will fail.
   *
   * <p>In ALL mode, these individual errors need to be ignored, but they should be logged at
   * warn level.</p>
   *
   * <p>In this case we have 2 nodes and 2 replicas configured, but we'll only get the result
   * back from the active and one replica.</p>
   */
  @Test
  @IgnoreWhen(
    nodesLessThan = 2,
    nodesGreaterThan = 2,
    replicasLessThan = 2,
    replicasGreaterThan = 2,
    clusterTypes = { ClusterType.MOCKED }
  )
  void ignoresErrorsOnNonAvailableReplicasInAllMode() throws Exception {
    final AtomicReference<IndividualReplicaGetFailedEvent> ev = new AtomicReference<>();
    environment.eventBus().subscribe(event -> {
      if (event instanceof IndividualReplicaGetFailedEvent) {
        ev.set((IndividualReplicaGetFailedEvent) event);
      }
    });

    String id = UUID.randomUUID().toString();
    collection.upsert(id, "Hello, World!");

    // TODO: perform a "replicate to 1" instead
    Thread.sleep(1000);

    List<GetResult> results = collection.getFromReplica(id).collect(Collectors.toList());
    assertEquals(2, results.size());
    assertNotNull(results.get(0));
    assertNotNull(results.get(1));
    assertEquals("Hello, World!", results.get(0).contentAs(String.class));
    assertEquals("Hello, World!", results.get(1).contentAs(String.class));

    waitUntilCondition(() -> ev.get() != null);
  }

}
