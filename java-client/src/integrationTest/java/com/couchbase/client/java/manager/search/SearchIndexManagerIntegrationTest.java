/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.search;

import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(missesCapabilities = Capabilities.SEARCH,
  clusterTypes = ClusterType.CAVES,
  clusterVersionIsBelow = ConsistencyUtil.CLUSTER_VERSION_MB_50101,
  isProtostellar = true
)
class SearchIndexManagerIntegrationTest extends JavaIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SearchIndexManagerIntegrationTest.class);


  private static Cluster cluster;

  private static SearchIndexManager indexes;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    indexes = cluster.searchIndexes();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.SEARCH);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void upsertAndGetIndex() throws Throwable {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), name);

    SearchIndex foundIndex = indexes.getIndex(name);
    assertEquals(name, foundIndex.name());
    assertEquals(config().bucketname(), foundIndex.sourceName());

    runWithRetry(Duration.ofSeconds(30), () -> {
      assertTrue(indexes.getIndexedDocumentsCount(name) >= 0);
      assertThrows(IndexNotFoundException.class, () -> indexes.getIndexedDocumentsCount("some-weird-index"));
    });
  }

  @Test
  void upsertTwice() {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), name);

    SearchIndex foundIndex = indexes.getIndex(name);

    indexes.upsertIndex(foundIndex);
  }

  @Test
  void getAllIndexes() {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), name);

    boolean found = false;
    List<SearchIndex> foundIndexes = indexes.getAllIndexes();
    for (SearchIndex foundIndex : foundIndexes) {
      if (foundIndex.name().equals(name)) {
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test
  void dropIndex() {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), name);

    SearchIndex foundIndex = indexes.getIndex(name);
    assertEquals(name, foundIndex.name());
    assertEquals(config().bucketname(), foundIndex.sourceName());

    indexes.dropIndex(name);
    ConsistencyUtil.waitUntilSearchIndexDropped(cluster.core(), name);

    assertThrows(IndexNotFoundException.class, () -> indexes.getIndex(name));
    assertThrows(IndexNotFoundException.class, () -> indexes.dropIndex(name));
  }

  /**
   * This test can only run against 6.5 and later, since that's when the analyze document
   * has been added.
   */
  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS)
  void analyzeDocument() throws Throwable {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), name);

    runWithRetry(Duration.ofSeconds(30), () -> {
      List<JsonObject> tokens = indexes.analyzeDocument(name, JsonObject.create().put("name", "hello world"));
      assertFalse(tokens.isEmpty());
    });
  }

  /**
   * This test performs various index management tasks on an index so it only needs to wait once that it comes
   * alive.
   */
  @Test
  void performVariousTasksOnIndex() throws Throwable {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), name);

    runWithRetry(Duration.ofSeconds(30), () -> {
      indexes.pauseIngest(name);
      indexes.resumeIngest(name);
      indexes.freezePlan(name);
      indexes.unfreezePlan(name);
      indexes.allowQuerying(name);
      indexes.disallowQuerying(name);

      assertThrows(IndexNotFoundException.class, () -> indexes.pauseIngest("some-weird-index"));
      assertThrows(IndexNotFoundException.class, () -> indexes.resumeIngest("some-weird-index"));
      assertThrows(IndexNotFoundException.class, () -> indexes.freezePlan("some-weird-index"));
      assertThrows(IndexNotFoundException.class, () -> indexes.unfreezePlan("some-weird-index"));
      assertThrows(IndexNotFoundException.class, () -> indexes.allowQuerying("some-weird-index"));
      assertThrows(IndexNotFoundException.class, () -> indexes.disallowQuerying("some-weird-index"));
    });
  }

  private static void runWithRetry(Duration timeout, Runnable task) throws Throwable {
    long startNanos = System.nanoTime();
    Throwable deferred = null;
    do {
      if (deferred != null) {
        // Don't spam the server ...
        MILLISECONDS.sleep(2000);
      }
      try {
        task.run();
        return;
      } catch (Throwable t) {
        LOGGER.warn("Retrying due to {}", t.toString()); // don't need stack trace
        deferred = t;
      }
    } while (System.nanoTime() - startNanos < timeout.toNanos());
    throw deferred;
  }

}
