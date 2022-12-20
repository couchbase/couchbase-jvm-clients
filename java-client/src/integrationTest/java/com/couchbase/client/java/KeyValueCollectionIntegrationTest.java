/*
 * Copyright 2020 Couchbase, Inc.
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
package com.couchbase.client.java;

import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS)
public class KeyValueCollectionIntegrationTest extends JavaIntegrationTest {

  static Cluster cluster;
  static Bucket bucket;

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    bucket = cluster.bucket(config().bucketname());
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void recognizesCollectionAfterCreation() {
    String collId = UUID.randomUUID().toString().substring(0, 10);
    CollectionSpec collectionSpec = CollectionSpec.create(collId, CollectionIdentifier.DEFAULT_SCOPE);
    bucket.collections().createCollection(collectionSpec);
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), bucket.name(), collectionSpec.scopeName(), collectionSpec.name());

    Collection collection = bucket.collection(collId);

    String id = UUID.randomUUID().toString();
    String content = "bar";
    MutationResult upsertResult = collection.upsert(id, content);
    GetResult getResult = collection.get(id);

    assertEquals(upsertResult.cas(), getResult.cas());
    assertEquals(content, getResult.contentAs(String.class));

    checkCachesScopesAndCollections(bucket.async(), collId);
  }

  private void checkCachesScopesAndCollections(AsyncBucket bucket, String collId) {
    assertSame(bucket.defaultScope(), bucket.defaultScope());
    assertSame(bucket.defaultScope(), bucket.scope("_default"));

    assertSame(bucket.defaultScope().defaultCollection(), bucket.defaultScope().defaultCollection());
    assertSame(bucket.defaultScope().defaultCollection(), bucket.scope("_default").collection("_default"));
    assertSame(bucket.defaultScope().collection(collId), bucket.defaultScope().collection(collId));
    assertNotSame(bucket.defaultScope().collection(collId), bucket.defaultScope().defaultCollection());
  }

  /**
   * Regression test for JVMCBC-874
   */
  @Test
  void usesCollectionNotFoundRetryReason() {
    Collection notFoundCollection = bucket.collection("doesNotExist");

    try {
      notFoundCollection.get("someDocumentId", getOptions().timeout(Duration.ofSeconds(5)));
      fail("Expected timeout!");
    } catch (UnambiguousTimeoutException ex) {
      assertTrue(ex.retryReasons().contains(RetryReason.COLLECTION_NOT_FOUND));
    }
  }
}
