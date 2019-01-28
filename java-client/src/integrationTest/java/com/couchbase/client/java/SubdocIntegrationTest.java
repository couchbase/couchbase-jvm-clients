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

import com.couchbase.client.java.env.ClusterEnvironment;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static com.couchbase.client.java.kv.LookupInOps.lookupInOps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubdocIntegrationTest extends JavaIntegrationTest {

  private Cluster cluster;
  private ClusterEnvironment environment;
  private Collection collection;

  @BeforeEach
  void beforeEach() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterEach
  void afterEach() {
    environment.shutdown(Duration.ofSeconds(1));
    cluster.shutdown();
  }

  @Test
  void emptyIfNotFound() {
    assertFalse(collection.lookupIn("does_not_exist", lookupInOps().get("foo")).isPresent());
  }

  @Test
  void loadPrimitives() {
    String id = UUID.randomUUID().toString();

    collection.upsert(
      id, JsonObject.create().put("foo", "bar").put("num", 1234)
    );

    Optional<LookupInResult> result = collection.lookupIn(id, lookupInOps().get("foo", "num"));
    assertTrue(result.isPresent());
    result.ifPresent(r -> {
      assertEquals("bar", r.contentAs(0, String.class));
      assertEquals(1234, (int) r.contentAs(1, Integer.class));
      assertTrue(r.exists(0));
      assertTrue(r.exists(1));
      assertFalse(r.exists(2));
      assertTrue(r.cas() != 0);
    });
  }

  @Test
  void loadObjectAndArray() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create()
      .put("obj", JsonObject.create())
      .put("arr", JsonArray.create())
    );

    Optional<LookupInResult> result = collection.lookupIn(id, lookupInOps().get("obj", "arr"));
    assertTrue(result.isPresent());
    result.ifPresent(r -> {
      assertEquals(JsonObject.empty(), r.contentAsObject(0));
      assertEquals(JsonArray.empty(), r.contentAsArray(1));
      assertTrue(r.exists(0));
      assertTrue(r.exists(1));
      assertTrue(r.cas() != 0);
    });
  }

}
