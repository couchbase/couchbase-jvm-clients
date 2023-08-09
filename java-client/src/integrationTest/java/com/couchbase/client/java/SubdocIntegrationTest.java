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
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException;
import com.couchbase.client.core.error.subdoc.PathInvalidException;
import com.couchbase.client.core.error.subdoc.PathMismatchException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.subdoc.XattrUnknownVirtualAttributeException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.util.CbCollections;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInMacro;
import com.couchbase.client.java.kv.LookupInReplicaResult;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.java.kv.LookupInSpec.get;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BooleanTypeRef extends TypeRef<Boolean> {
}

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
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void invalidPath() {
    String docId = UUID.randomUUID().toString();
    collection.upsert(docId, emptyMap());

    Class<? extends Throwable> expected = config().isProtostellar() ? InvalidArgumentException.class : PathInvalidException.class;

    assertThrows(expected, () ->
      collection.lookupIn(
        docId,
        listOf(LookupInSpec.get("x[")) // syntax error
      ).contentAs(0, String.class)
    );
  }

  @Test
  // Needs ING-383
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true)
  void tooManyCommands() {
    String docId = UUID.randomUUID().toString();

    List<LookupInSpec> specs = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      specs.add(LookupInSpec.get("foo" + i));
    }

    CouchbaseException e = assertThrows(CouchbaseException.class, () ->
      collection.lookupIn(docId, specs)
    );

    if (!hasCause(e, InvalidArgumentException.class)) {
      e.printStackTrace();
      fail("Expected cause to be InvalidArgumentException, but got the above stack trace.");
    }
  }

  @Test
  @IgnoreWhen(isProtostellarWillWorkLater = true)
  void xattrOrder() {
    String docId = UUID.randomUUID().toString();
    collection.upsert(docId, mapOf("magicWord", "xyzzy"));

    // Must be okay for xattr lookups to come after normal lookups
    LookupInResult result = collection.lookupIn(docId, listOf(
      LookupInSpec.get("magicWord"),
      LookupInSpec.get("someXattr").xattr()
    ));

    assertEquals("xyzzy", result.contentAs(0, String.class));
    assertFalse(result.exists(1));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void notJson() {
    String docId = UUID.randomUUID().toString();
    collection.upsert(
      docId,
      "I am not JSON!",
      UpsertOptions.upsertOptions()
        .transcoder(RawStringTranscoder.INSTANCE)
    );

    // Overall request succeeds
    LookupInResult result = collection.lookupIn(
      docId,
      listOf(LookupInSpec.get("x"))
    );

    // Touching the items triggers the exception
    assertThrows(DocumentNotJsonException.class, () -> result.contentAs(0, String.class));

    // Arguably should throw DocumentNotJsonException, but this is the current behavior, so...
    assertFalse(result.exists(0));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void notJsonMulti() {
    String docId = UUID.randomUUID().toString();
    collection.upsert(
      docId,
      "I am not JSON!",
      UpsertOptions.upsertOptions()
        .transcoder(RawStringTranscoder.INSTANCE)
    );

    // Overall request succeeds
    LookupInResult result = collection.lookupIn(
      docId,
      listOf(
        LookupInSpec.get("x"),
        LookupInSpec.get("y")
      )
    );

    // Touching the items triggers the exception
    assertThrows(DocumentNotJsonException.class, () -> result.contentAs(0, String.class));
    assertThrows(DocumentNotJsonException.class, () -> result.contentAs(1, String.class));

    // Arguably should throw DocumentNotJsonException, but this is the current behavior, so...
    assertFalse(result.exists(0));
    assertFalse(result.exists(1));
  }

  @Test
  void resultIndexOutOfBounds() {
    String docId = UUID.randomUUID().toString();
    collection.upsert(docId, emptyMap());

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class, () ->
      collection.lookupIn(
        docId,
        listOf(LookupInSpec.get("x"))
      ).contentAs(1, String.class) // invalid index!
    );

    assertEquals("Index 1 is out of bounds; must be >= 0 and < 1", e.getMessage());
  }

  @Test
  void existsSingle() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create().put("foo", "bar"));

    LookupInResult result = collection.lookupIn(id, Collections.singletonList(LookupInSpec.exists("not_exist")));

    assertFalse(result.exists(0));

    // Bug: https://issues.couchbase.com/browse/JCBC-2056
    assertThrows(PathNotFoundException.class, () -> result.contentAs(0, Boolean.class));
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
      listOf(
        LookupInSpec.exists("not_exist"),
        LookupInSpec.get("foo"),
        LookupInSpec.exists("foo")
      )
    );

    assertFalse(result.exists(0));

    // Bug: https://issues.couchbase.com/browse/JCBC-2056
    assertThrows(PathNotFoundException.class, () -> result.contentAs(0, Boolean.class));
    assertThrows(PathNotFoundException.class, () -> result.contentAsBytes(0));

    assertTrue(result.exists(1));
    assertEquals("bar", result.contentAs(1, String.class));

    assertTrue(result.exists(2));
    assertTrue(result.contentAs(2, Boolean.class));
    assertEquals("true", result.contentAs(2, String.class));
    assertArrayEquals("true".getBytes(UTF_8), result.contentAsBytes(2));
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
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAllReplicasBlocking() throws InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    waitForReplicaResult(() -> {
      Stream<LookupInReplicaResult> result = collection.lookupInAllReplicas(id, Collections.singletonList(get("foo")));
      return result.peek(r -> assertEquals(content.get("foo"), r.contentAs(0, String.class))).collect(Collectors.toList());
    });
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAnyReplicasTooManyBlocking() throws InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    assertThrows(DocumentUnretrievableException.class, () -> collection.lookupInAnyReplica(id, CbCollections.listOf(get("1"), get("2"), get("3"), get("4"), get("5"), get("6"), get("7"), get("8"), get("9"), get("10"), get("11"), get("12"), get("13"), get("14"), get("15"), get("16"), get("17"))));
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  @Disabled
    // Needs wholedoc get - see https://issues.couchbase.com/browse/MB-23162
  void getFullDocumentAllReplicasBlocking() throws InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    waitForReplicaResult(() -> {
      Stream<LookupInReplicaResult> result = collection.lookupInAllReplicas(id, Collections.singletonList(get("")));
      return result.peek(r -> assertEquals(content.get("foo"), r.contentAs(0, String.class))).collect(Collectors.toList());
    });
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAllReplicasNotFoundBlocking() {

    String id = UUID.randomUUID().toString();

    Stream<LookupInReplicaResult> result = collection.lookupInAllReplicas(id, Collections.singletonList(get("foo")));
    assertEquals(0, result.count());
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAnyReplicaBlocking() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    LookupInReplicaResult result = collection.lookupInAnyReplica(id, Collections.singletonList(get("foo")));
    assertEquals(content.get("foo"), result.contentAs(0, String.class));
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAllReplicasAsync() throws ExecutionException, InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    List<LookupInReplicaResult> resultList = waitForReplicaResult(() -> {
      CompletableFuture<List<CompletableFuture<LookupInReplicaResult>>> result = collection.async().lookupInAllReplicas(id, Collections.singletonList(get("foo")));
      List<LookupInReplicaResult> collect;
      try {
        collect = result.get().stream().map(r -> {
          try {
            return r.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return collect;
    });

    assertEquals(content.get("foo"), resultList.get(0).contentAs(0, String.class));
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAnyReplicaAsync() throws ExecutionException, InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    CompletableFuture<LookupInReplicaResult> future = collection.async().lookupInAnyReplica(id, Collections.singletonList(get("foo")));
    LookupInReplicaResult result = future.get();
    assertEquals(content.get("foo"), result.contentAs(0, String.class));
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAllReplicasReactive() throws InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    waitForReplicaResult(() -> {
      Flux<LookupInReplicaResult> result = collection.reactive().lookupInAllReplicas(id, Collections.singletonList(get("foo")));
      return result.map(r -> {
        assertEquals(content.get("foo"), r.contentAs(0, String.class));
        return r;
      }).collectList().block();
    });
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAnyReplicaReactive() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);

    Mono<LookupInReplicaResult> result = collection.reactive().lookupInAnyReplica(id, Collections.singletonList(get("foo")));
    LookupInReplicaResult replica = result.block();
    assertEquals(content.get("foo"), replica.contentAs(0, String.class));
    collection.remove(id);
  }

  private List<LookupInReplicaResult> waitForReplicaResult(Supplier<List<LookupInReplicaResult>> func) throws InterruptedException {
    final int MAX_REPLICA_TRIES = 20;
    List<LookupInReplicaResult> resultList;
    int tries = 0;
    do {
      MILLISECONDS.sleep(100);
      resultList = func.get();
    } while (resultList.size() < config().numReplicas() + 1 && ++tries <= MAX_REPLICA_TRIES);
    if (resultList.size() != config().numReplicas() + 1) {
      throw new WrongNumberOfReplicasException(("expected " + (config().numReplicas() + 1) + " found " + resultList.size()));
    }
    return resultList;
  }

  private class WrongNumberOfReplicasException extends RuntimeException {
    public WrongNumberOfReplicasException(String s) {
      super(s);
    }
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
    } catch (CasMismatchException err) {
      errorCount += 1;
    }

    try {
      collection.mutateIn(id,
        Collections.singletonList(MutateInSpec.upsert("mutated", 2)),
        MutateInOptions.mutateInOptions()
          .cas(gr.cas())
          .durability(DurabilityLevel.MAJORITY));
    } catch (CasMismatchException err) {
      errorCount += 1;
    }

    assertEquals(1, errorCount);
  }

  @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
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

  @IgnoreWhen(clusterTypes = {ClusterType.MOCKED}, missesCapabilities = {Capabilities.GLOBAL_CONFIG})
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

  @Test
  void existsAcceptsInvalidIndex() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, mapOf());

    LookupInResult result = collection.lookupIn(id, listOf(
      LookupInSpec.exists("doesNotExist")
    ));

    // This behavior is not according to spec, but it's what the
    // Java SDK currently does.
    assertFalse(result.exists(7));
  }

  @Test
  void existsDoesNotPropagateExceptions() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, mapOf("foo", mapOf("bar", "zor")));

    LookupInResult result = collection.lookupIn(id, listOf(
      LookupInSpec.get("foo.bar[0]")
    ));

    assertThrows(PathMismatchException.class, () -> result.contentAsBytes(0));

    // Spec says `exists` should propagate all exceptions except "PATH_NOT_FOUND",
    // just like `get` or `count`.
    // Java SDK diverges from spec; it treats all errors as "does not exit".
    assertFalse(result.exists(0));
  }

  @Test
  void reactiveSmokeTest() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, mapOf());

    MutateInResult mutate = collection.reactive().mutateIn(id, listOf(MutateInSpec.upsert("foo", "bar"))).block();
    assertNotNull(mutate);

    LookupInResult lookup = collection.reactive().lookupIn(id, listOf(LookupInSpec.get("foo"))).block();
    assertNotNull(lookup);
    assertEquals("bar", lookup.contentAs(0, String.class));
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAllReplicasTooManyBlocking() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);
    List<LookupInSpec> specs = new ArrayList<>();
    for (int i = 0; i < 17; i++) {
      specs.add(LookupInSpec.get("x" + i));
    }
    assertEquals(emptyList(), collection.lookupInAllReplicas(id, specs).collect(Collectors.toList()));
    collection.remove(id);
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6.0")
  void getDocumentAllReplicasTooManyBlocking2() throws InterruptedException {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create().put("foo", "bar");
    collection.upsert(id, content);
    List<LookupInSpec> specs = new ArrayList<>();
    specs.add(LookupInSpec.get("foo"));
    specs.add(LookupInSpec.get("x"));

    List<LookupInReplicaResult> list = waitForReplicaResult(() -> collection.lookupInAllReplicas(id, specs).collect(Collectors.toList()));
    list.forEach(r -> {
      assertEquals("bar", r.contentAs(0, String.class));
      assertThrows(PathNotFoundException.class, () -> r.contentAsBytes(1));
    });
    collection.remove(id);
  }

  @Test
  @Timeout(2)
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void getDocumentAnyReplicasTooManyBlockingNotTimeout() throws InterruptedException {
    List<LookupInSpec> specs = new ArrayList<>();
    for (int i = 0; i < 17; i++) {
      specs.add(LookupInSpec.get("x" + i));
    }
    assertThrows(DocumentUnretrievableException.class, () -> collection.lookupInAnyReplica("airline_10", specs));
  }
}
