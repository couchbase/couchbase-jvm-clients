/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests the GetOrNull feature.
 *
 * Note this feature is well-tested by FIT, so we only test the parts that does not test here (the async API).
 */
class GetOrNullIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;

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
  void getOrNullNotFoundAsync() throws ExecutionException, InterruptedException {
      GetResult getResult = collection.async().getOrNull("bad").get();
      assertNull(getResult);
  }

  @Test
  void getOrNullFoundAsync() throws ExecutionException, InterruptedException {
    String docId = UUID.randomUUID().toString();
    collection.insert(docId, JsonObject.create());

    GetResult getResult = collection.async().getOrNull(docId).get();
    assertNotNull(getResult);
  }
}
