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

import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SubdocIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(connectionString(), clusterOptions());
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void emptyIfNotFound() {
    assertThrows(
      KeyNotFoundException.class,
      () -> collection.lookupIn("does_not_exist", Collections.singletonList(LookupInSpec.get("foo")))
    );
  }

  @Test
  void loadPrimitives() {
    String id = UUID.randomUUID().toString();

    collection.upsert(
      id, JsonObject.create().put("foo", "bar").put("num", 1234)
    );

    LookupInResult result = collection.lookupIn(id, Arrays.asList(LookupInSpec.get("foo"), LookupInSpec.get("num")));
    assertEquals("bar", result.contentAs(0, String.class));
    assertEquals(1234, (int) result.contentAs(1, Integer.class));
    assertTrue(result.exists(0));
    assertTrue(result.exists(1));
    assertFalse(result.exists(2));
    assertTrue(result.cas() != 0);
  }

  @Test
  void loadObjectAndArray() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create()
      .put("obj", JsonObject.create())
      .put("arr", JsonArray.create())
    );

    LookupInResult result = collection.lookupIn(
      id,
      Arrays.asList(LookupInSpec.get("obj"), LookupInSpec.get("arr"))
    );
    assertEquals(JsonObject.empty(), result.contentAsObject(0));
    assertEquals(JsonArray.empty(), result.contentAsArray(1));
    assertTrue(result.exists(0));
    assertTrue(result.exists(1));
    assertTrue(result.cas() != 0);
  }

  @Test
  void insertPrimitive() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.empty());

    MutateInResult result = collection.mutateIn(
      id,
      Collections.singletonList(MutateInSpec.insert("foo", "bar"))
    );
    assertTrue(result.cas() != 0);

    assertEquals(
      JsonObject.create().put("foo", "bar"),
      collection.get(id).contentAsObject()
    );
  }

  @Test
  void pathDoesNotExistSingle() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.empty());

    assertThrows(
      PathNotFoundException.class,
      () -> collection.lookupIn(id, Collections.singletonList(LookupInSpec.get("not_exist")))
    );
  }

  @Test
  void pathDoesNotExistMulti() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"));

    LookupInResult result = collection.lookupIn(id, Arrays.asList(LookupInSpec.get("not_exist"), LookupInSpec.get("foo")));

    assertFalse(result.exists(0));
    assertTrue(result.exists(1));
    assertThrows(PathNotFoundException.class, () ->
      assertTrue(result.contentAs(0, Boolean.class))
    );
    assertEquals("bar", result.contentAs(1, String.class));
  }


  @Test
  void doNotFetchExpiration() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"),
            UpsertOptions.upsertOptions()
                    .expiry(Duration.ofSeconds(60)));

    LookupInResult result = collection.lookupIn(id,
            Arrays.asList(
                    LookupInSpec.get("not_exist"),
                    LookupInSpec.get("foo")));

    assertFalse(result.expiry().isPresent());
  }

  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
  void fetchExpiration() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"),
            UpsertOptions.upsertOptions()
                    .expiry(Duration.ofSeconds(60)));

    LookupInResult result = collection.lookupIn(id,
            Arrays.asList(
                    LookupInSpec.get("not_exist"),
                    LookupInSpec.get("foo")),
            LookupInOptions.lookupInOptions().withExpiry(true));

    assertTrue(result.expiry().isPresent());
  }

  // TODO this throws and shouldn't. need to implement single subdoc path. check old client AsyncLookupInBuilder
//  @Test
//  void existsSingle() {
//    String id = UUID.randomUUID().toString();
//
//    collection.upsert(id, JsonObject.create().put("foo", "bar"));
//
//    LookupInResult result = collection.lookupIn(id, lookupInOps().exists("not_exist")).get();
//
//    assertFalse(result.exists(0));
//    assertThrows(PathNotFoundException.class, () ->
//            assertTrue(result.contentAs(0, Boolean.class))
//    );
//  }

  @Test
  void existsMulti() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"));


    LookupInResult result = collection.lookupIn(
      id,
      Arrays.asList(LookupInSpec.exists("not_exist"), LookupInSpec.get("foo"))
    );

    assertFalse(result.exists(0));
    assertThrows(
      PathNotFoundException.class,
      () -> assertTrue(result.contentAs(0, Boolean.class))
    );

    assertTrue(result.exists(1));
    assertEquals("bar", result.contentAs(1, String.class));
  }

  @Test
  void count() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", JsonArray.from("hello", "world")));

    LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.count("foo")));

    assertTrue(result.exists(0));
    assertEquals(2, (int) result.contentAs(0, Integer.class));
  }


  @Test
  void getFullDocument() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.get("")));
    assertEquals(content, result.contentAsObject(0));
  }

  @Test
  void upsertFullDocument() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");

    collection.mutateIn(id,
      Arrays.asList(
        // Server doesn't allow fulLDocument to be only op here, get "key not found"
        MutateInSpec.upsert("qix", "qux"),
        MutateInSpec.upsert("", content)
      ),
      MutateInOptions.mutateInOptions().upsertDocument(true)
    );

    GetResult doc = collection.get(id);
    assertEquals(content, doc.contentAsObject());
  }

  @Test
  void insertFullDocument() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");

    collection.mutateIn(id,
      Arrays.asList(
        // Server doesn't allow fulLDocument to be only op here, get "key not found"
        MutateInSpec.upsert("qix", "qux"),
        MutateInSpec.upsert("", content)
      ),
      MutateInOptions.mutateInOptions().insertDocument(true)
    );

    GetResult doc = collection.get(id);

    assertEquals(content, doc.contentAsObject());
  }


  @Test
  void counterMulti() {
    JsonObject initial = JsonObject.create()
            .put("mutated", 0)
            .put("body", "")
            .put("first_name", "James")
            .put("age", 0);

    String id = UUID.randomUUID().toString();
    collection.upsert(id, initial);

    MutateInResult result = collection.mutateIn(id,
            Arrays.asList(
                    MutateInSpec.upsert("addr", JsonObject.create()
                            .put("state", "NV")
                            .put("pincode", 7)
                            .put("city", "Chicago")),
                    MutateInSpec.increment("mutated", 1),
                    MutateInSpec.upsert("name", JsonObject.create()
                            .put("last", "")
                            .put("first", "James")
                    )
            ));

    assertEquals(1, result.contentAs(1, Integer.class));
  }

  @Test
  void counterSingle() {
    JsonObject initial = JsonObject.create()
            .put("mutated", 0)
            .put("body", "")
            .put("first_name", "James")
            .put("age", 0);

    String id = UUID.randomUUID().toString();
    collection.upsert(id, initial);

    MutateInResult result = collection.mutateIn(id,
            Arrays.asList(
                    MutateInSpec.increment("mutated", 1)
            ));

    assertEquals(1, result.contentAs(0, Integer.class));
  }

  // JVMCBC-728
  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SYNC_REPLICATION)
  void subdocCASWithDurability() {
    JsonObject initial = JsonObject.create().put("mutated", 0);
    String id = UUID.randomUUID().toString();
    collection.upsert(id, initial);

    GetResult gr = collection.get(id);

    int errorCount = 0;

    try {
      MutateInResult result = collection.mutateIn(id,
              Arrays.asList(MutateInSpec.upsert("mutated", 1)),
              MutateInOptions.mutateInOptions()
                      .cas(gr.cas())
                      .durability(DurabilityLevel.MAJORITY));
    }
    catch (CASMismatchException err) {
      errorCount += 1;
    }

    try {
      MutateInResult result = collection.mutateIn(id,
              Arrays.asList(MutateInSpec.upsert("mutated", 2)),
              MutateInOptions.mutateInOptions()
                      .cas(gr.cas())
                      .durability(DurabilityLevel.MAJORITY));
    }
    catch (CASMismatchException err) {
      errorCount += 1;
    }

    assertEquals(1, errorCount);
  }

}
