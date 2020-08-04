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
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
class SearchIndexManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  private static SearchIndexManager indexes;

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    Bucket bucket = cluster.bucket(config().bucketname());
    indexes = cluster.searchIndexes();
    bucket.waitUntilReady(Duration.ofSeconds(5));
    waitForService(bucket, ServiceType.SEARCH);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void upsertAndGetIndex() throws Exception {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);

    SearchIndex foundIndex = indexes.getIndex(name);
    assertEquals(name, foundIndex.name());
    assertEquals(config().bucketname(), foundIndex.sourceName());

    // TODO: fixme
    Thread.sleep(2000);

    assertTrue(indexes.getIndexedDocumentsCount(name) >= 0);
    assertThrows(IndexNotFoundException.class, () -> indexes.getIndexedDocumentsCount("some-weird-index"));
  }

  @Test
  void upsertTwice() {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);

    SearchIndex foundIndex = indexes.getIndex(name);

    indexes.upsertIndex(foundIndex);
  }

  @Test
  void getAllIndexes() {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);

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

    SearchIndex foundIndex = indexes.getIndex(name);
    assertEquals(name, foundIndex.name());
    assertEquals(config().bucketname(), foundIndex.sourceName());

    indexes.dropIndex(name);

    assertThrows(IndexNotFoundException.class, () -> indexes.getIndex(name));
    assertThrows(IndexNotFoundException.class, () -> indexes.dropIndex(name));
  }

  /**
   * This test can only run against 6.5 and later, since that's when the analyze document
   * has been added.
   */
  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS)
  void analyzeDocument() throws Exception {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);

    // TODO: FIXME
    Thread.sleep(2000);

    List<JsonObject> tokens = indexes.analyzeDocument(name, JsonObject.create().put("name", "hello world"));
    assertFalse(tokens.isEmpty());
  }

  /**
   * This test performs various index management tasks on an index so it only needs to wait once that it comes
   * alive.
   */
  @Test
  void performVariousTasksOnIndex() throws Exception {
    String name = "idx-" + UUID.randomUUID().toString().substring(0, 8);
    SearchIndex index = new SearchIndex(name, config().bucketname());
    indexes.upsertIndex(index);

    // TODO: FIXME
    Thread.sleep(2000);

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
  }

}
