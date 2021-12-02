/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.client.test.Caves;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@IgnoreWhen(clusterTypes = { ClusterType.CONTAINERIZED, ClusterType.MOCKED, ClusterType.UNMANAGED, ClusterType.CAVES})
class CavesKeyValueIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;

  @BeforeAll
  static void beforeAll() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(5));
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Caves("kv/crud/SetGet")
  @Test
  void setGet() {
    JsonObject testDoc = JsonObject
      .create()
      .put("foo", "bar");

    collection.upsert("test-doc", testDoc);

    GetResult result = collection.get("test-doc");
    assertEquals(testDoc, result.contentAsObject());
  }

}
