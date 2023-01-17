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

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.subdoc.XattrUnknownVirtualAttributeException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

import com.couchbase.client.java.codec.TypeRef;
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

import static com.couchbase.client.java.kv.LookupInSpec.get;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class BooleanTypeRef extends TypeRef<Boolean> {
}

@IgnoreWhen(isProtostellarWillWorkLater = true)
class SubdocIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void emptyIfNotFound() {
    assertThrows(
      DocumentNotFoundException.class,
      () -> collection.lookupIn("does_not_exist", Collections.singletonList(LookupInSpec.get("foo")))
    );
  }

  @Test
  void loadPrimitives() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar").put("num", 1234));

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
      Arrays.asList(get("obj"), get("arr"))
    );
    assertEquals(JsonObject.create(), result.contentAsObject(0));
    assertEquals(JsonArray.create(), result.contentAsArray(1));
    assertArrayEquals("{}".getBytes(UTF_8), result.contentAsBytes(0));
    assertTrue(result.exists(0));
    assertTrue(result.exists(1));
    assertTrue(result.cas() != 0);
  }

  @Test
  void insertPrimitive() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create());

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

    collection.upsert(id, JsonObject.create());

    assertThrows(
      PathNotFoundException.class,
      () -> collection.lookupIn(id, Collections.singletonList(LookupInSpec.get("not_exist"))).contentAsObject(0)
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
  void existsSingle() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"));

    LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.exists("not_exist")));

    assertFalse(result.exists(0));
    assertThrows(PathNotFoundException.class, () -> assertTrue(result.contentAs(0, Boolean.class)));
  }

  @Test
  void doesExistSingle() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"));

    LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.exists("foo")));

    assertTrue(result.exists(0));
    assertTrue(result.contentAs(0, Boolean.class));
    assertTrue(result.contentAs(0, new BooleanTypeRef()));
  }

  @Test
  void existsMulti() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"));


    LookupInResult result = collection.lookupIn(
      id,
      Arrays.asList(LookupInSpec.exists("not_exist"), LookupInSpec.get("foo"), LookupInSpec.exists("foo"))
    );

    assertFalse(result.exists(0));
    assertThrows(
      PathNotFoundException.class,
      () -> assertTrue(result.contentAs(0, Boolean.class))
    );

    assertTrue(result.exists(1));
    assertEquals("bar", result.contentAs(1, String.class));

    assertTrue(result.exists(2));
    assertTrue(result.contentAs(2, Boolean.class));
    assertThrows(
            IllegalArgumentException.class,
            () -> result.contentAs(2, String.class));
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
        // Server doesn't allow fullDocument to be only op here, get "key not found"
        MutateInSpec.upsert("qix", "qux"),
        MutateInSpec.replace("", content)
      ),
      MutateInOptions.mutateInOptions().storeSemantics(StoreSemantics.UPSERT)
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
        MutateInSpec.replace("", content)
      ),
      MutateInOptions.mutateInOptions().storeSemantics(StoreSemantics.INSERT)
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
      Collections.singletonList(
        MutateInSpec.increment("mutated", 1)
      ));

    assertEquals(1, result.contentAs(0, Integer.class));
  }

  // JVMCBC-728
  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SYNC_REPLICATION, clusterTypes = ClusterType.CAVES)
  void subdocCASWithDurability() {
    JsonObject initial = JsonObject.create().put("mutated", 0);
    String id = UUID.randomUUID().toString();
    collection.upsert(id, initial);

    GetResult gr = collection.get(id);

    int errorCount = 0;

    try {
      collection.mutateIn(id,
        Collections.singletonList(MutateInSpec.upsert("mutated", 1)),
              MutateInOptions.mutateInOptions()
                      .cas(gr.cas())
                      .durability(DurabilityLevel.MAJORITY));
    }
    catch (CasMismatchException err) {
      errorCount += 1;
    }

    try {
      collection.mutateIn(id,
        Collections.singletonList(MutateInSpec.upsert("mutated", 2)),
              MutateInOptions.mutateInOptions()
                      .cas(gr.cas())
                      .durability(DurabilityLevel.MAJORITY));
    }
    catch (CasMismatchException err) {
      errorCount += 1;
    }

    assertEquals(1, errorCount);
  }

  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED })
  @Test
  void macros() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.create());

    LookupInResult result = collection.lookupIn(
      id,
      Arrays.asList(
        get(LookupInMacro.DOCUMENT).xattr(),
        get(LookupInMacro.CAS).xattr(),
        get(LookupInMacro.IS_DELETED).xattr(),
        get(LookupInMacro.SEQ_NO).xattr(),
        get(LookupInMacro.VALUE_SIZE_BYTES).xattr(),
        get(LookupInMacro.EXPIRY_TIME).xattr()
      )
    );

    result.contentAs(0, JsonObject.class);
    result.contentAs(1, String.class);
    assertFalse(result.contentAs(2, Boolean.class));
    result.contentAs(3, String.class);
    result.contentAs(4, Integer.class);
    result.contentAs(5, Integer.class);
  }

  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED }, missesCapabilities = { Capabilities.GLOBAL_CONFIG })
  @Test
  void madHatterMacros() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.create());

    LookupInResult result = collection.lookupIn(id, Collections.singletonList(get(LookupInMacro.REV_ID).xattr()));
    result.contentAs(0, String.class);
  }

  // This test can be run on any cluster < 6.5.1, but there's not a clean way of identifying that.
  // Instead use SYNC_REP as a proxy, so it will be run on any cluster < 6.5.0
  @Test
  @IgnoreWhen(hasCapabilities = Capabilities.SYNC_REPLICATION)
  void vbucketVattrRaisesClientSideError() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.create());

    assertThrows(XattrUnknownVirtualAttributeException.class, () ->
            collection.lookupIn(id, Arrays.asList(LookupInSpec.get("$vbucket").xattr())));
  }
}
