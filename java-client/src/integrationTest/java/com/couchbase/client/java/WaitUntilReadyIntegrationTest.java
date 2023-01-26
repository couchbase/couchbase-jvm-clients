/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.Flaky;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@IgnoreWhen(isProtostellarWillWorkLater = true)
public class WaitUntilReadyIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Consumer<ClusterEnvironment.Builder> environment;
  private static SimpleEventBus eventBus;

  @BeforeAll
  static void beforeAll() {
    eventBus = new SimpleEventBus(true);
    environment = env -> env.eventBus(eventBus);
    cluster = createCluster(environment);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
    eventBus = null;
  }

  @Test
  void timesOutClusterWhenNotReady() {
    Cluster cluster =  Cluster.connect(
      "127.0.0.1",
      ClusterOptions.clusterOptions("foo", "bar").environment(environment)
    );

    assertThrows(UnambiguousTimeoutException.class, () -> cluster.waitUntilReady(Duration.ofSeconds(2)));
    cluster.disconnect();
  }

  @Test
  void timesOutBucketWhenNotReady() {
    Cluster cluster =  Cluster.connect(
      "127.0.0.1",
      ClusterOptions.clusterOptions("foo", "bar").environment(environment)
    );
    Bucket bucket = cluster.bucket("foo");
    assertThrows(UnambiguousTimeoutException.class, () -> bucket.waitUntilReady(Duration.ofSeconds(2)));
    cluster.disconnect();
  }

  @Test
  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED, ClusterType.CAVES, ClusterType.CAPELLA })
  void handlesCreatingBucketDuringWaitUntilReady()  {
    ExecutorService es = Executors.newFixedThreadPool(1);
    String bucketName = UUID.randomUUID().toString();

    if (!config().capabilities().contains(Capabilities.GLOBAL_CONFIG)) {
      // We need to open the "other" bucket to make sure the test passes in clusters pre 6.5
      Bucket b = cluster.bucket(config().bucketname());
      b.waitUntilReady(Duration.ofSeconds(30));
    }

    long creationDelay = 2000;
    try {
      Bucket bucket = cluster.bucket(bucketName);
      es.submit(() -> {
        try {
          Thread.sleep(creationDelay);
        } catch (InterruptedException e) {
          fail();
        }
        cluster.buckets().createBucket(BucketSettings.create(bucketName));
      });

      long start = System.nanoTime();
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
      long end = System.nanoTime();

      Collection collection = bucket.defaultCollection();
      collection.upsert("my-doc", JsonObject.create());
      assertEquals(JsonObject.create(), collection.get("my-doc").contentAsObject());

      assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) > creationDelay);
    } finally {
      es.shutdownNow();
      try {
        cluster.buckets().dropBucket(bucketName);
      } catch (BucketNotFoundException ex) {
        // ignore
      }
    }
  }

  // Seeing this fail on CI on the upsert with timeouts due to repeated KV_TEMPORARY_FAILURE
  @Disabled
  @Flaky
  @RepeatedTest(3) // first time often succeeds regardless
  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED, ClusterType.CAVES })
  void waitsForNewlyCreatedBucket() {
    String bucketName = UUID.randomUUID().toString();
    Cluster cluster = createCluster();

    if (!config().capabilities().contains(Capabilities.GLOBAL_CONFIG)) {
      // We need to open the "other" bucket to make sure the test passes in clusters pre 6.5
      Bucket b = cluster.bucket(config().bucketname());
      b.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    }

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
      cluster.buckets().createBucket(BucketSettings.create(bucketName).ramQuotaMB(100));
      Bucket bucket = cluster.bucket(bucketName);
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
      Collection collection = bucket.defaultCollection();
      collection.upsert("foo", "bar", UpsertOptions.upsertOptions().timeout(Duration.ofMillis(2500)));
    } finally {
      try {
        cluster.buckets().dropBucket(bucketName);
      } catch (BucketNotFoundException ignore) {
      } finally {
        cluster.disconnect();
      }
    }
  }

}
