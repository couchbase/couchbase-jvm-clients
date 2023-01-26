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

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ReplicaNotConfiguredException;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class ObserveIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection collection;

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  void persistToActive() {
    String id = UUID.randomUUID().toString();

    MutationResult result = collection.insert(
      id,
      "some value",
      insertOptions().durability(PersistTo.ACTIVE, ReplicateTo.NONE)
    );
    assertTrue(result.cas() != 0);
    assertTrue(result.mutationToken().isPresent());
  }

  @Test
  void removePersistToActive() {
    String id = UUID.randomUUID().toString();

    MutationResult result = collection.insert(
      id,
      "some value",
      insertOptions().durability(PersistTo.ACTIVE, ReplicateTo.NONE)
    );
    assertTrue(result.cas() != 0);


    MutationResult removeResult = collection.remove(
      id,
      removeOptions().durability(PersistTo.ACTIVE, ReplicateTo.NONE)
    );
    assertTrue(removeResult.cas() != 0);
    assertTrue(result.cas() != removeResult.cas());
    assertTrue(result.mutationToken().isPresent());
  }

  @Test
  @IgnoreWhen(replicasGreaterThan = 1)
  void failsFastIfTooManyReplicasRequested() {
    String value = "some value";
    assertThrows(
      ReplicaNotConfiguredException.class,
      () -> collection.insert(
        UUID.randomUUID().toString(),
        value,
        insertOptions().durability(PersistTo.THREE, ReplicateTo.NONE)
      )
    );
    assertThrows(
      ReplicaNotConfiguredException.class,
      () -> collection.insert(
        UUID.randomUUID().toString(),
        value,
        insertOptions().durability(PersistTo.NONE, ReplicateTo.TWO)
      )
    );
    assertThrows(
      ReplicaNotConfiguredException.class,
      () -> collection.insert(
        UUID.randomUUID().toString(),
        value,
        insertOptions().durability(PersistTo.FOUR, ReplicateTo.THREE)
      )
    );
  }

  @Test
  @IgnoreWhen(replicasLessThan = 1, nodesGreaterThan = 1)
  void timesOutIfReplicaNotAvailableWithBestEffort() {
    String id = UUID.randomUUID().toString();

    assertThrows(RuntimeException.class, () -> collection.insert(
      id,
      "some value",
      insertOptions().durability(PersistTo.NONE, ReplicateTo.ONE).timeout(Duration.ofSeconds(1))
    ));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void disallowObserveWhenTokensDisabled() {
    Cluster cluster = createCluster(env -> env.ioConfig(IoConfig.enableMutationTokens(false)));
    try {
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();

      waitUntilCondition(() -> cluster.core().clusterConfig().hasClusterOrBucketConfig());

      assertThrows(
        FeatureNotAvailableException.class,
        () -> collection.upsert(
          UUID.randomUUID().toString(),
          "bar",
          upsertOptions().durability(PersistTo.ACTIVE, ReplicateTo.NONE)
        )
      );
    } finally {
      cluster.disconnect();
    }
  }

}
