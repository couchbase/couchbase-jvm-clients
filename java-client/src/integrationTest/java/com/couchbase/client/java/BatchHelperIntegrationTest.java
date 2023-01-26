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

import com.couchbase.client.java.batch.BatchHelper;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the {@link BatchHelper}.
 */
@IgnoreWhen(clusterTypes = ClusterType.CAVES,
  isProtostellarWillWorkLater = true
)
public class BatchHelperIntegrationTest extends JavaIntegrationTest {

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
  void performsBatchExists() {
    List<String> idsThatExist = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String  id = UUID.randomUUID().toString();
      idsThatExist.add(id);
      collection.upsert(id, JsonObject.create());
    }

    List<String> toCheck = new ArrayList<>(idsThatExist);
    toCheck.add("some-other");
    toCheck.add("id-that-doesnt-exist");

    List<String> found = BatchHelper.exists(collection, toCheck);
    Collections.sort(idsThatExist);
    Collections.sort(found);
    assertEquals(idsThatExist, found);
  }

  @Test
  void performsBatchGetIfExists() {
    List<String> idsThatExist = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String  id = UUID.randomUUID().toString();
      idsThatExist.add(id);
      collection.upsert(id, JsonObject.create());
    }

    List<String> toCheck = new ArrayList<>(idsThatExist);
    toCheck.add("some-other");
    toCheck.add("id-that-doesnt-exist");

    Map<String, GetResult> found = BatchHelper.getIfExists(collection, toCheck);
    assertEquals(found.size(), idsThatExist.size());
    for (Map.Entry<String, GetResult> entry : found.entrySet()) {
      assertTrue(idsThatExist.contains(entry.getKey()));
      assertEquals(JsonObject.create(), entry.getValue().contentAsObject());
    }
  }

  @Test
  void performsBatchWhenNoValueFound() {
    List<String> toCheck = Arrays.asList("foo", "bar", "baz");
    Map<String, GetResult> found = BatchHelper.getIfExists(collection, toCheck);
    assertTrue(found.isEmpty());
  }

}
