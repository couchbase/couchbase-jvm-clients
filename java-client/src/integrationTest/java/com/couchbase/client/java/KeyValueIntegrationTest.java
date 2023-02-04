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
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.java.kv.DecrementOptions.decrementOptions;
import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.IncrementOptions.incrementOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This integration test makes sure the various KV-based APIs work as they are intended to.
 *
 * <p>Note that specialized tests which assert some kind of special environment should be placed in
 * separate files for better debuggability.</p>
 *
 * @since 3.0.0
 */
class KeyValueIntegrationTest extends JavaIntegrationTest {

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
  void insertAndGet() {
    String id = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(id, "Hello, World");

    assertTrue(insertResult.cas() != 0);
    assertTrue(insertResult.mutationToken().isPresent());

    GetResult getResult = collection.get(id);
    assertEquals("Hello, World", getResult.contentAs(String.class));
    assertEquals("\"Hello, World\"", new String(getResult.contentAsBytes(), UTF_8));
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiryTime().isPresent());
  }


  @Test
  void requestCancelledPostShutdown() {
    Cluster cluster1 = createCluster();
    Collection coll = cluster1.bucket(config().bucketname()).defaultCollection();
    cluster1.disconnect();

    assertThrows(RequestCanceledException.class, () -> {
      String id = UUID.randomUUID().toString();
      coll.insert(id, "Hello, World");
    });
  }

  /**
   * Mock does not support Get Meta, so we need to ignore it there.
   */
  @Test
  @IgnoreWhen( clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true) // Needs ING-362
  void exists() {
    String id = UUID.randomUUID().toString();

    assertFalse(collection.exists(id).exists());

    MutationResult insertResult = collection.insert(id, "Hello, World");
    assertTrue(insertResult.cas() != 0);

    ExistsResult existsResult = collection.exists(id);

    assertEquals(insertResult.cas(), existsResult.cas());
    assertTrue(existsResult.exists());
    assertFalse(collection.exists("some_id").exists());
  }

  @Test
  void emptyIfGetNotFound() {
    assertThrows(DocumentNotFoundException.class, () -> collection.get(UUID.randomUUID().toString()));
  }

  @Test
  void emptyIfGetNotFoundReactive() {
    assertThrows(DocumentNotFoundException.class, () -> collection.reactive().get(UUID.randomUUID().toString()).block());
  }

  @Test
  void errorContextIsPopulated() {
    try {
      collection.get(UUID.randomUUID().toString());
    }
    catch (DocumentNotFoundException err) {
      Map<String, Object> input = new HashMap<>();
      err.context().injectExportableParams(input);
      assertEquals(true, input.get("idempotent"));
      assertEquals(0, input.get("retried"));
      assertEquals(2500L, input.get("timeoutMs"));
      assertFalse(input.containsKey("cancelled"));
      assertFalse(input.containsKey("reason"));
      assertFalse(input.containsKey("retryReasons"));
      assertTrue(input.containsKey("timings"));
    }
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void getWithProjection() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("age", 12);

    MutationResult mutationResult = collection.upsert(id, content);
    assertTrue(mutationResult.cas() != 0);

    GetResult getResult = collection.get(id, getOptions().project("foo", "created", "notfound"));
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiryTime().isPresent());

    JsonObject decoded = getResult.contentAsObject();
    assertEquals("bar", decoded.getString("foo"));
    assertEquals(true, decoded.getBoolean("created"));
    assertFalse(decoded.containsKey("age"));
  }

  /**
   * Verify that an empty Optional is returned even though the expiry is requested.
   */
  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-369
  @Test
  void expiryRequestedButNotSetOnDoc() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("age", 12);

    MutationResult mutationResult = collection.upsert(id, content);
    assertTrue(mutationResult.cas() != 0);

    GetResult getResult = collection.get(id, getOptions().withExpiry(true));
    assertNoExpiry(id);
  }

  /**
   * Right now the mock does not support xattr/macro expansion so this test is
   * ignored on the mock. Once the mock supports it, please remove the ignore
   * annotation.
   *
   * <p>See https://github.com/couchbase/CouchbaseMock/issues/46</p>
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)  // Needs ING-369
  void fullDocWithExpiration() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("age", 12);

    MutationResult mutationResult = collection.upsert(
      id,
      content,
      upsertOptions().expiry(Duration.ofSeconds(5))
    );
    assertTrue(mutationResult.cas() != 0);

    GetResult getResult = collection.get(id, getOptions().withExpiry(true));
    assertTrue(getResult.expiryTime().isPresent());
    assertTrue(getResult.expiryTime().get().toEpochMilli() > 0);
    assertEquals(content, getResult.contentAsObject());
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true)  // Needs ING-369
  void fullDocWithExpirationAndCustomTranscoder() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("age", 12);

    MutationResult mutationResult = collection.upsert(
      id,
      content.toBytes(),
      upsertOptions().expiry(Duration.ofSeconds(5)).transcoder(RawBinaryTranscoder.INSTANCE)
    );
    assertTrue(mutationResult.cas() != 0);


    GetResult getResult = collection.get(id, getOptions().withExpiry(true).transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(getResult.expiryTime().isPresent());
    assertTrue(getResult.expiryTime().get().toEpochMilli() > 0);
    assertEquals(content, JsonObject.fromJson(getResult.contentAs(byte[].class)));
  }

  /**
   * Right now the mock does not support xattr/macro expansion so this test is
   * ignored on the mock. Once the mock supports it, please remove the ignore
   * annotation.
   *
   * <p>See https://github.com/couchbase/CouchbaseMock/issues/46</p>
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)
  void projectionWithExpiration() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("age", 12);

    MutationResult mutationResult = collection.upsert(
      id,
      content,
      upsertOptions().expiry(Duration.ofSeconds(5))
    );
    assertTrue(mutationResult.cas() != 0);

    GetResult getResult = collection.get(
      id,
      getOptions().project("foo", "created").withExpiry(true)
    );
    assertTrue(getResult.cas() != 0);
    assertTrue(getResult.expiryTime().isPresent());
    assertTrue(getResult.expiryTime().get().toEpochMilli() > 0);

    JsonObject decoded = getResult.contentAsObject();
    assertEquals("bar", decoded.getString("foo"));
    assertEquals(true, decoded.getBoolean("created"));
    assertFalse(decoded.containsKey("age"));
  }

  /**
   * We need to ignore this test on the mock because the mock returns TMPFAIL instead of LOCKED when the
   * document is locked (which used to be the old functionality but now since XERROR is negotiated it
   * returns LOCKED properly).
   *
   * <p>Once the mock is modified to return LOCKED, this test can also be run on the mock again.</p>
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true) // Needs ING-370
  void getAndLock() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(id, expected);

    assertTrue(insert.cas() != 0);

    GetResult getAndLock = collection.getAndLock(id, Duration.ofSeconds(30));

    assertTrue(getAndLock.cas() != 0);
    assertNotEquals(insert.cas(), getAndLock.cas());
    assertEquals(expected, getAndLock.contentAsObject());

    TimeoutException exception = assertThrows(
      TimeoutException.class,
      () -> collection.getAndLock(id, Duration.ofSeconds(30), getAndLockOptions().timeout(Duration.ofMillis(100)))
    );
    assertEquals(EnumSet.of(RetryReason.KV_LOCKED), exception.context().requestContext().retryReasons());
    assertThrows(DocumentNotFoundException.class, () -> collection.getAndLock("some_doc", Duration.ofSeconds(30)));
  }

  /**
   * This test is ignored against the mock because right now it does not bump the CAS like
   * the server does when getAndTouch is called.
   *
   * <p>Remove the ignore as soon as https://github.com/couchbase/CouchbaseMock/issues/49 is
   * fixed.</p>
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true) // Needs ING-370
  void getAndTouch() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(
      id,
      expected,
      insertOptions().expiry(Duration.ofSeconds(10))
    );
    assertTrue(insert.cas() != 0);

    GetResult getAndTouch = collection.getAndTouch(id, Duration.ofSeconds(1));

    assertTrue(getAndTouch.cas() != 0);
    assertNotEquals(insert.cas(), getAndTouch.cas());
    assertEquals(expected, getAndTouch.contentAsObject());

    waitUntilCondition(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // ignored.
      }
      try {
        collection.get(id);
        return false;
      } catch (DocumentNotFoundException knf) {
        return true;
      }
    });
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-355
  @Test
  void remove() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(
      id,
      expected,
      insertOptions().expiry(Duration.ofSeconds(2))
    );
    assertTrue(insert.cas() != 0);

    assertThrows(
      CasMismatchException.class,
      () -> collection.remove(id, removeOptions().cas(insert.cas() + 100))
    );

    MutationResult result = collection.remove(id);
    assertTrue(result.cas() != insert.cas());

    assertThrows(DocumentNotFoundException.class, () -> collection.remove(id));
  }

  /**
   * Regression test for JCBC-1587 (deleted flag is not honored after a previous remove).
   */
  @Test
  @IgnoreWhen( clusterTypes = ClusterType.MOCKED, isProtostellarWillWorkLater = true) // Needs ING-362
  void existsReturnsFalseAfterRemove() {
    String id = UUID.randomUUID().toString();

    assertFalse(collection.exists(id).exists());

    collection.upsert(id, JsonObject.create());
    assertTrue(collection.exists(id).exists());

    collection.remove(id);
    assertFalse(collection.exists(id).exists());
  }

  @Test
  void insert() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(id, expected);
    assertTrue(insert.cas() != 0);

    assertThrows(DocumentExistsException.class, () -> collection.insert(id, expected));
  }

  @Test
  void upsert() {
    String id = UUID.randomUUID().toString();

    MutationResult upsert = collection.upsert(
      id,
      JsonObject.create().put("foo", true)
    );
    assertTrue(upsert.cas() != 0);

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult upsertOverride = collection.upsert(id, expected);
    assertTrue(upsertOverride.cas() != 0);
    assertTrue(upsert.cas() != upsertOverride.cas());

    assertEquals(expected, collection.get(id).contentAsObject());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-355
  @Test
  void replace() {
    String id = UUID.randomUUID().toString();

    MutationResult upsert = collection.upsert(
      id,
      JsonObject.create().put("foo", true)
    );
    assertTrue(upsert.cas() != 0);

    assertThrows(CasMismatchException.class, () -> collection.replace(
        id,
        JsonObject.create(),
        replaceOptions().cas(upsert.cas() + 1))
    );

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult replaced = collection.replace(id, expected);
    assertTrue(replaced.cas() != 0);
    assertTrue(upsert.cas() != replaced.cas());

    assertEquals(expected, collection.get(id).contentAsObject());

    assertThrows(
      DocumentNotFoundException.class,
      () -> collection.replace("some_doc", JsonObject.create())
    );
  }

  /**
   *  It seems the mock will not actually upsert a doc when the expiry exceeds 30 days.
   *  So https://github.com/couchbase/CouchbaseMock/issues/58 needs to be fixed, at
   *  which time we can remove the restriction on not running this test when mocked.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)  // Needs ING-369
  void checkExpiryBeyond2038() {
    // The server interprets the 32-bit expiry field as an unsigned
    // integer. This means the maximum value is 4294967295 seconds,
    // which corresponds to 2106-02-07T06:28:15Z.
    //
    // This test will start to fail when the current time is less than
    // 30 years from that maximum expiration instant.

    int expiryYears = 30;

    // make sure we are not time travelers.
    LocalDate expirationDate = LocalDate.now().plusYears(expiryYears);
    assertTrue(expirationDate.getYear() > 2038);

    checkExpiry(Duration.ofDays(365 * expiryYears));
  }

  @Test
  @IgnoreWhen(clusterTypes = {ClusterType.MOCKED}, isProtostellarWillWorkLater = true)  // Needs ING-369
  void checkExpiryExactly30Days() {
    checkExpiry(Duration.ofDays(30));
  }

  /**
   * 30 days is the cutoff where the server starts interpreting
   * the expiry value as an epoch second instead of a duration. This test
   * ensures we're shielding the user from that surprising behavior.
   */
  @Test
  @IgnoreWhen(clusterTypes = {ClusterType.MOCKED}, isProtostellarWillWorkLater = true)  // Needs ING-369
  void checkExpiryBeyond30Days() {
    checkExpiry(Duration.ofDays(31));
  }

  void checkExpiry(Duration expiryDuration) {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, JsonObject.create(), upsertOptions().expiry(expiryDuration));
    assertExpiry(id, expiryDuration);
    collection.remove(id);

    collection.insert(id, JsonObject.create(), InsertOptions.insertOptions().expiry(expiryDuration));
    assertExpiry(id, expiryDuration);
    collection.remove(id);

    collection.upsert(id, JsonObject.create());
    collection.replace(id, JsonObject.create(), replaceOptions().expiry(expiryDuration));
    assertExpiry(id, expiryDuration);
    collection.remove(id);

    collection.upsert(id, JsonObject.create());
    collection.touch(id, expiryDuration);
    assertExpiry(id, expiryDuration);
    collection.remove(id);

    collection.upsert(id, JsonObject.create());
    collection.getAndTouch(id, expiryDuration);
    assertExpiry(id, expiryDuration);
    collection.remove(id);
  }

  void assertExpiry(String documentId, Duration expiryDuration) {
    GetResult result = collection.get(documentId, GetOptions.getOptions().withExpiry(true));

    Instant actualExpiry = result.expiryTime().orElseThrow(() -> new AssertionError("expected expiry"));
    Instant expectedExpiry = Instant.ofEpochMilli(System.currentTimeMillis()).plus(expiryDuration);

    long secondsDifference = actualExpiry.getEpochSecond() - expectedExpiry.getEpochSecond();
    long acceptanceThresholdSeconds = Duration.ofMinutes(5).getSeconds();

    assertTrue(Math.abs(secondsDifference) < acceptanceThresholdSeconds);
  }

  private static void assertExpiry(String docId, Instant expectedExpiry) {
    assertEquals(
        Optional.of(expectedExpiry.truncatedTo(SECONDS)),
        collection.get(docId, GetOptions.getOptions().withExpiry(true)).expiryTime()
    );
  }

  private static void assertNoExpiry(String docId) {
    assertEquals(
      Optional.empty(),
      collection.get(docId, GetOptions.getOptions().withExpiry(true)).expiryTime()
    );
  }

  private static final Instant NEAR_FUTURE_INSTANT = Instant.now().plus(5, DAYS);

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.PRESERVE_EXPIRY, clusterTypes = ClusterType.CAVES, isProtostellarWillWorkLater = true) // Need ING-369
  void upsertCanPreserveExpiry() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "foo", upsertOptions()
        .expiry(NEAR_FUTURE_INSTANT)
        .preserveExpiry(true)
    );
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.upsert(id, "bar", upsertOptions()
        .expiry(NEAR_FUTURE_INSTANT.plus(5, DAYS))
        .preserveExpiry(true)
    );
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.upsert(id, "bar", upsertOptions()
        .preserveExpiry(true)
    );
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.upsert(id, "bar");
    assertNoExpiry(id);
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.PRESERVE_EXPIRY, clusterTypes = ClusterType.CAVES, isProtostellarWillWorkLater = true) // Need ING-369
  void replaceCanPreserveExpiry() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "foo", upsertOptions().expiry(NEAR_FUTURE_INSTANT));

    collection.replace(id, "bar", replaceOptions().preserveExpiry(true));
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.replace(id, "boo");
    assertNoExpiry(id);
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.PRESERVE_EXPIRY, clusterTypes = ClusterType.CAVES, isProtostellarWillWorkLater = true) // No Protostellar support currently
  void subdocCanPreserveExpiry() {
    String id = UUID.randomUUID().toString();

    collection.mutateIn(id, listOf(MutateInSpec.upsert("foo", "bar")), mutateInOptions()
        .storeSemantics(StoreSemantics.INSERT)
        .expiry(NEAR_FUTURE_INSTANT)
    );
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.mutateIn(id, listOf(MutateInSpec.upsert("foo", "bar")), mutateInOptions()
        .storeSemantics(StoreSemantics.UPSERT)
        .expiry(NEAR_FUTURE_INSTANT.plus(5, DAYS))
        .preserveExpiry(true)
    );
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.mutateIn(id, listOf(MutateInSpec.upsert("foo", "bar")), mutateInOptions()
        .storeSemantics(StoreSemantics.REPLACE)
        .preserveExpiry(true)
    );
    assertExpiry(id, NEAR_FUTURE_INSTANT);

    collection.mutateIn(id, listOf(MutateInSpec.upsert("foo", "bar")));
    assertNoExpiry(id);
  }

  /**
   * Right now the mock does not change the cas on touch, so we need to ignore the test
   * until https://github.com/couchbase/CouchbaseMock/issues/50 is resolved.
   */
  @Test
  @IgnoreWhen(clusterTypes = {ClusterType.MOCKED}, isProtostellarWillWorkLater = true) // Needs ING-370
  void touch() throws Exception {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult upsert = collection.upsert(
      id,
      expected,
      upsertOptions().expiry(Duration.ofSeconds(10))
    );
    assertTrue(upsert.cas() != 0);

    MutationResult touched = collection.touch(id, Duration.ofSeconds(1));
    assertNotEquals(touched.cas(), upsert.cas());

    Thread.sleep(300);

    GetResult r = collection.get(id);
    assertEquals(expected, r.contentAsObject());
    assertEquals(r.cas(), touched.cas());

    waitUntilCondition(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // ignored.
      }
      try {
        collection.get(id);
        return false;
      } catch (DocumentNotFoundException knf) {
        return true;
      }
    });
  }

  /**
   * The mock returns TMPFAIL instead of LOCKED, so this test is ignored on the mock.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true) // Needs ING-370
  void unlock() {
    String id = UUID.randomUUID().toString();

    MutationResult upsert = collection.upsert(id, JsonObject.create().put("foo", true));
    assertTrue(upsert.cas() != 0);

    GetResult locked = collection.getAndLock(id, Duration.ofSeconds(30));

    TimeoutException exception = assertThrows(
      TimeoutException.class,
      () -> collection.upsert(id, JsonObject.create(), upsertOptions().timeout(Duration.ofMillis(100))));
    assertEquals(EnumSet.of(RetryReason.KV_LOCKED), exception.context().requestContext().retryReasons());

    assertThrows(CasMismatchException.class, () -> collection.unlock(id, locked.cas() + 1));

    collection.unlock(id, locked.cas());

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult replaced = collection.replace(id, expected);
    assertTrue(replaced.cas() != 0);

    assertEquals(expected, collection.get(id).contentAsObject());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void append() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

    assertThrows(
      DocumentNotFoundException.class,
      () -> collection.binary().append(id, helloBytes)
    );

    MutationResult upsert = collection.upsert(id, helloBytes, upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );

    MutationResult append = collection.binary().append(id, worldBytes);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      helloWorldBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void appendReactive() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

    assertThrows(
            DocumentNotFoundException.class,
            () -> collection.reactive().binary().append(id, helloBytes).block()
    );

    MutationResult upsert = collection.upsert(id, helloBytes, upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
            helloBytes,
            collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );

    MutationResult append = collection.reactive().binary().append(id, worldBytes).block();
    assertNotNull(append);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
            helloWorldBytes,
            collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );
  }

  @Test
  void appendAsync() throws ExecutionException, InterruptedException {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

   assertThrows(
            ExecutionException.class,
            () -> collection.async().binary().append(id, helloBytes).get()
   );

    MutationResult upsert = collection.upsert(id, helloBytes, upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
            helloBytes,
            collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );

    MutationResult append = collection.async().binary().append(id, worldBytes).get();
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
            helloWorldBytes,
            collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void prepend() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] worldHelloBytes = "World!Hello, ".getBytes(UTF_8);

    assertThrows(
      DocumentNotFoundException.class,
      () -> collection.binary().prepend(id, helloBytes)
    );

    MutationResult upsert = collection.upsert(id, helloBytes, upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );

    MutationResult append = collection.binary().prepend(id, worldBytes);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      worldHelloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void prependReactive() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] worldHelloBytes = "World!Hello, ".getBytes(UTF_8);

    assertThrows(
    DocumentNotFoundException.class,
     () -> collection.reactive().binary().prepend(id, helloBytes).block()
    );

    MutationResult upsert = collection.upsert(id, helloBytes, upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );

    MutationResult append = collection.reactive().binary().prepend(id, worldBytes).block();
    assertNotNull(append);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      worldHelloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );
  }

  @Test
  void prependAsync() throws ExecutionException, InterruptedException {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] worldHelloBytes = "World!Hello, ".getBytes(UTF_8);

    assertThrows(
      ExecutionException.class,
      () -> collection.async().binary().append(id, helloBytes).get()
    );

    MutationResult upsert = collection.upsert(id, helloBytes, upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );

    MutationResult append = collection.async().binary().prepend(id, worldBytes).get();
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      worldHelloBytes,
      collection.get(id, getOptions().transcoder(RawBinaryTranscoder.INSTANCE)).contentAs(byte[].class)
    );
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void increment() {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentNotFoundException.class, () -> collection.binary().increment(id));

    CounterResult result = collection.binary().increment(
      id,
      incrementOptions().initial(5L)
    );

    assertTrue(result.cas() != 0);
    assertEquals(5L, result.content());

    result = collection.binary().increment(id, incrementOptions().delta(2));

    assertTrue(result.cas() != 0);
    assertEquals(7L, result.content());

    result = collection.binary().increment(id, incrementOptions());

    assertTrue(result.cas() != 0);
    assertEquals(8L, result.content());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void incrementAsync() throws ExecutionException, InterruptedException {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentNotFoundException.class, () -> collection.binary().increment(id));

    CounterResult result = collection.async().binary().increment(
      id,
      incrementOptions().initial(5L)
    ).get();

    assertTrue(result.cas() != 0);
    assertEquals(5L, result.content());

    result = collection.async().binary().increment(id, incrementOptions().delta(2)).get();

    assertTrue(result.cas() != 0);
    assertEquals(7L, result.content());

    result = collection.async().binary().increment(id, incrementOptions()).get();

    assertTrue(result.cas() != 0);
    assertEquals(8L, result.content());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void incrementReactive() {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentNotFoundException.class, () -> collection.binary().increment(id));

    CounterResult result = collection.reactive().binary().increment(
      id,
      incrementOptions().initial(5L)
    ).block();

    assertTrue(result.cas() != 0);
    assertEquals(5L, result.content());

    result = collection.reactive().binary().increment(id, incrementOptions().delta(2)).block();

    assertTrue(result.cas() != 0);
    assertEquals(7L, result.content());

    result = collection.reactive().binary().increment(id, incrementOptions()).block();

    assertTrue(result.cas() != 0);
    assertEquals(8L, result.content());
  }

  /**
   * Right now the mock allows the value to be decremented below zero, which is against the server
   * spec/protocol. Once https://github.com/couchbase/CouchbaseMock/issues/51 is fixed, this
   * ignore annotation can be removed.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)
  void decrement() {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentNotFoundException.class, () -> collection.binary().decrement(id));

    CounterResult result = collection.binary().decrement(
      id,
      decrementOptions().initial(2L)
    );

    assertTrue(result.cas() != 0);
    assertEquals(2L, result.content());

    result = collection.binary().decrement(id);

    assertTrue(result.cas() != 0);
    assertEquals(1L, result.content());

    result = collection.binary().decrement(id);

    assertTrue(result.cas() != 0);
    assertEquals(0L, result.content());

    result = collection.binary().decrement(id);

    assertTrue(result.cas() != 0);
    assertEquals(0L, result.content());
  }

  /**
   * Right now the mock allows the value to be decremented below zero, which is against the server
   * spec/protocol. Once https://github.com/couchbase/CouchbaseMock/issues/51 is fixed, this
   * ignore annotation can be removed.
   */

  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)
  void decrementAsync() throws ExecutionException, InterruptedException {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentNotFoundException.class, () -> collection.binary().decrement(id));

    CounterResult result = collection.async().binary().decrement(
      id,
      decrementOptions().initial(2L)
    ).get();

    assertTrue(result.cas() != 0);
    assertEquals(2L, result.content());

    result = collection.async().binary().decrement(id).get();

    assertTrue(result.cas() != 0);
    assertEquals(1L, result.content());

    result = collection.async().binary().decrement(id).get();

    assertTrue(result.cas() != 0);
    assertEquals(0L, result.content());

    result = collection.async().binary().decrement(id).get();

    assertTrue(result.cas() != 0);
    assertEquals(0L, result.content());
  }

  /**
   * Right now the mock allows the value to be decremented below zero, which is against the server
   * spec/protocol. Once https://github.com/couchbase/CouchbaseMock/issues/51 is fixed, this
   * ignore annotation can be removed.
   */

  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED }, isProtostellarWillWorkLater = true)
  void decrementReactive() {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentNotFoundException.class, () -> collection.reactive().binary().decrement(id).block());

    CounterResult result = collection.reactive().binary().decrement(
      id,
      decrementOptions().initial(2L)
    ).block();

    assertTrue(result.cas() != 0);
    assertEquals(2L, result.content());

    result = collection.reactive().binary().decrement(id).block();

    assertTrue(result.cas() != 0);
    assertEquals(1L, result.content());

    result = collection.reactive().binary().decrement(id).block();

    assertTrue(result.cas() != 0);
    assertEquals(0L, result.content());

    result = collection.reactive().binary().decrement(id).block();

    assertTrue(result.cas() != 0);
    assertEquals(0L, result.content());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true)
  @Test
  void throwsIfTooLarge() {
    String id = UUID.randomUUID().toString();
    JsonObject content = JsonObject.create();
    for (int i = 0; i < 400000; i++) {
      content.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    assertThrows(ValueTooLargeException.class, () -> collection.upsert(id, content, upsertOptions().timeout(Duration.ofSeconds(30))));
  }

  @IgnoreWhen(hasCapabilities = Capabilities.CREATE_AS_DELETED)
  @Test
  void requestForCreateAsDeletedShouldFailInClient() {
    assertThrows(FeatureNotAvailableException.class, () -> {
      String id = UUID.randomUUID().toString();
      collection.mutateIn(id, Collections.singletonList(
                MutateInSpec.insert("txn", JsonObject.create()).xattr()
              ),
              mutateInOptions()
              .storeSemantics(StoreSemantics.UPSERT)
              .createAsDeleted(true));
    });
  }

}
