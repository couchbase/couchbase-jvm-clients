/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.errors;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetAndTouchOptions.getAndTouchOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.TouchOptions.touchOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests many aspects of the exception raising for the KeyValue service.
 */
class KeyValueErrorIntegrationTest extends JavaIntegrationTest {

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

  // This fails as no client-side projection length checks - ideally would be centralised in STG.
  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyGetExceptions() {
    DocumentNotFoundException thrown = assertThrows(
      DocumentNotFoundException.class,
      () -> collection.get(UUID.randomUUID().toString())
    );
    assertNotNull(thrown.context());

    assertThrows(InvalidArgumentException.class, () -> collection.get("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.get(""));
    assertThrows(InvalidArgumentException.class, () -> collection.get(null));
    assertThrows(InvalidArgumentException.class, () -> collection.get("", getOptions().withExpiry(true)));

    List<String> tooManyFields = IntStream.rangeClosed(1, 17).boxed().map(Object::toString).collect(Collectors.toList());
    assertThrows(InvalidArgumentException.class, () -> collection.get("foo", getOptions().project(tooManyFields)));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyGetAndLockExceptions() {
    DocumentNotFoundException thrown = assertThrows(
      DocumentNotFoundException.class,
      () -> collection.getAndLock(UUID.randomUUID().toString(), Duration.ofSeconds(1))
    );
    assertNotNull(thrown.context());

    assertThrows(InvalidArgumentException.class, () -> collection.getAndLock("foo", Duration.ofSeconds(1), null));
    assertThrows(InvalidArgumentException.class, () -> collection.getAndLock("", Duration.ofSeconds(1)));
    assertThrows(InvalidArgumentException.class, () -> collection.getAndLock(null, Duration.ofSeconds(1)));
    assertThrows(InvalidArgumentException.class, () -> collection.getAndLock("foo", null));
  }

  /**
   * The mock still returns tmpfail but we want to check that the rerty reason is actually locked as it should
   * be post 5.0.
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true)
  void verifyGetAndLockDoubleLock() {
    String validId = UUID.randomUUID().toString();
    collection.upsert(validId, JsonObject.create());
    collection.getAndLock(validId, Duration.ofSeconds(5));
    TimeoutException exception = assertThrows(
      TimeoutException.class,
      () -> collection.getAndLock(validId, Duration.ofSeconds(5), getAndLockOptions().timeout(Duration.ofSeconds(1)))
    );
    assertTrue(exception.context().requestContext().retryReasons().contains(RetryReason.KV_LOCKED));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyGetAndTouchExceptions() {
    DocumentNotFoundException thrown = assertThrows(
      DocumentNotFoundException.class,
      () -> collection.getAndTouch(UUID.randomUUID().toString(), Duration.ofSeconds(1))
    );
    assertNotNull(thrown.context());

    assertThrows(InvalidArgumentException.class, () -> collection.getAndTouch("foo", Duration.ofSeconds(1), null));
    assertThrows(InvalidArgumentException.class, () -> collection.getAndTouch("", Duration.ofSeconds(1)));
    assertThrows(InvalidArgumentException.class, () -> collection.getAndTouch(null, Duration.ofSeconds(1)));
    assertThrows(InvalidArgumentException.class, () -> collection.getAndTouch("foo", null));
  }

  /**
   * The mock still returns tmpfail but we want to check that the rerty reason is actually locked as it should
   * be post 5.0.
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true)
  void verifyTouchingLocked() {
    String validId = UUID.randomUUID().toString();
    collection.upsert(validId, JsonObject.create());
    collection.getAndLock(validId, Duration.ofSeconds(5));
    TimeoutException exception = assertThrows(
      TimeoutException.class,
      () -> collection.getAndTouch(validId, Duration.ofSeconds(2), getAndTouchOptions().timeout(Duration.ofSeconds(1)))
    );
    assertTrue(exception.context().requestContext().retryReasons().contains(RetryReason.KV_LOCKED));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyExistsExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.exists("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.exists(null));
  }

  @Test
  void verifyRemoveExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.remove("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.remove(null));
  }

  @Test
  void verifyInsertExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.insert("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.insert(null, "bar"));
    assertThrows(InvalidArgumentException.class, () -> collection.insert("foo", "bar", null));

    String id = UUID.randomUUID().toString();
    collection.insert(id, "bar");

    DocumentExistsException thrown = assertThrows(
      DocumentExistsException.class,
      () -> collection.insert(id, "bar")
    );
    assertNotNull(thrown.context());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyUpsertExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.upsert("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.upsert(null, "bar"));
    assertThrows(InvalidArgumentException.class, () -> collection.upsert("foo", "bar", null));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyReplaceExceptions() {
    DocumentNotFoundException thrown = assertThrows(
      DocumentNotFoundException.class,
      () -> collection.replace(UUID.randomUUID().toString(), "bar")
    );
    assertNotNull(thrown.context());

    assertThrows(InvalidArgumentException.class, () -> collection.replace("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.replace(null, "bar"));
    assertThrows(InvalidArgumentException.class, () -> collection.replace("foo", "bar", null));

    String id = UUID.randomUUID().toString();
    MutationResult result = collection.upsert(id, "bar");

    CasMismatchException mismatch = assertThrows(
      CasMismatchException.class,
      () -> collection.replace(id, "bar", replaceOptions().cas(result.cas() + 1))
    );
    assertNotNull(mismatch.context());
  }

  @Test
  void verifyTouchExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.touch("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.touch(null, Duration.ofSeconds(1), null));
    assertThrows(InvalidArgumentException.class, () -> collection.touch("foo", null, touchOptions()));

    DocumentNotFoundException thrown = assertThrows(
      DocumentNotFoundException.class,
      () -> collection.touch(UUID.randomUUID().toString(), Duration.ofSeconds(1))
    );
    assertNotNull(thrown.context());
  }

  @Test
  void verifyUnlockExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.unlock(null, 0));
    assertThrows(InvalidArgumentException.class, () -> collection.unlock("foo", 0));
    assertThrows(InvalidArgumentException.class, () -> collection.unlock("foo", 0, null));

    DocumentNotFoundException thrown = assertThrows(
      DocumentNotFoundException.class,
      () -> collection.unlock(UUID.randomUUID().toString(), 1)
    );
    assertNotNull(thrown.context());
  }

  /**
   * Ignored for the mock because it still returns TMPFAIL (like the old servers)
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true)
  void verifyUnlockCasMismatch() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "foo");
    GetResult result = collection.getAndLock(id, Duration.ofSeconds(5));

    CasMismatchException thrown = assertThrows(
      CasMismatchException.class,
      () -> collection.unlock(id, result.cas() + 1)
    );
    assertNotNull(thrown.context());
  }

  @Test
  void verifyLookupInExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.lookupIn(null, null));
    assertThrows(InvalidArgumentException.class, () -> collection.lookupIn("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.lookupIn("foo", Collections.emptyList()));
    assertThrows(InvalidArgumentException.class, () -> collection.lookupIn(
      "foo",
      Collections.singletonList(LookupInSpec.get("foo")),
      null
    ));
  }

  @Test
  void verifyMutateInExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.mutateIn(null, null));
    assertThrows(InvalidArgumentException.class, () -> collection.mutateIn("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.mutateIn("foo", Collections.emptyList()));
    assertThrows(InvalidArgumentException.class, () -> collection.mutateIn(
      "foo",
      Collections.singletonList(MutateInSpec.insert("foo", "bar")),
      null
    ));
  }

  @Test
  void verifyGetAllReplicasExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.getAllReplicas(null));
    assertThrows(InvalidArgumentException.class, () -> collection.getAllReplicas("foo", null));
    assertEquals(0, collection.getAllReplicas(UUID.randomUUID().toString()).count());
  }

  @Test
  void verifyGetAnyReplicaExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.getAnyReplica(null));
    assertThrows(InvalidArgumentException.class, () -> collection.getAnyReplica("foo", null));
  }

  @Test
  void verifyAppendExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.binary().append(null, null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().append("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().append("foo", new byte[] {}, null));
  }

  @Test
  void verifyPrependExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.binary().prepend(null, null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().prepend("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().prepend("foo", new byte[] {}, null));
  }

  @Test
  void verifyIncrementExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.binary().increment(null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().increment(null, null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().increment("foo", null));
  }

  @Test
  void verifyDecrementExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.binary().decrement(null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().decrement(null, null));
    assertThrows(InvalidArgumentException.class, () -> collection.binary().decrement("foo", null));
  }

  @Test
  void verifyListExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.list("id", null));
    assertThrows(InvalidArgumentException.class, () -> collection.list(null, Integer.class));
    assertThrows(InvalidArgumentException.class, () -> collection.list("id", Integer.class, null));
  }

  @Test
  void verifySetExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.set("id", null));
    assertThrows(InvalidArgumentException.class, () -> collection.set(null, Integer.class));
    assertThrows(InvalidArgumentException.class, () -> collection.set("id", Integer.class, null));
  }

  @Test
  void verifyMapExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.map("id", null));
    assertThrows(InvalidArgumentException.class, () -> collection.map(null, Integer.class));
    assertThrows(InvalidArgumentException.class, () -> collection.map("id", Integer.class, null));
  }

  @Test
  void verifyQueueExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.queue("id", null));
    assertThrows(InvalidArgumentException.class, () -> collection.queue(null, Integer.class));
    assertThrows(InvalidArgumentException.class, () -> collection.queue("id", Integer.class, null));
  }

  /**
   * The default key length is 250, including the collection information.
   */
  // This fails sa no client-side key length check, which would maybe require encoding the collection id to do correctly, so ideally would be done in STG.
  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void verifyTooLongId() {
    String longId = "The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. " +
      "The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. " +
      "The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.";
    assertThrows(InvalidArgumentException.class, () -> collection.get(longId));
  }

}
