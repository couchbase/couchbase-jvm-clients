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
import com.couchbase.client.core.error.KeyExistsException;
import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.core.error.RequestTimeoutException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.codec.BinaryContent;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.java.kv.DecrementOptions.decrementOptions;
import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.IncrementOptions.incrementOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.UnlockOptions.unlockOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  static private ClusterEnvironment environment;
  static private Collection collection;

  @BeforeAll
  static void beforeAll() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterAll
  static void afterAll() {
    cluster.shutdown();
    environment.shutdown();
  }

  @Test
  void insertAndGet() {
    String id = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(id, "Hello, World");

    assertTrue(insertResult.cas() != 0);
    assertTrue(insertResult.mutationToken().isPresent());

    GetResult getResult = collection.get(id);
    assertEquals("Hello, World", getResult.contentAs(String.class));
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiration().isPresent());
  }

  @Test
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
    assertThrows(KeyNotFoundException.class, () -> collection.get(UUID.randomUUID().toString()));
  }

  @Test
  void getWithProjection() {
    String id = UUID.randomUUID().toString();

    JsonObject content = JsonObject.create()
      .put("foo", "bar")
      .put("created", true)
      .put("age", 12);

    MutationResult mutationResult = collection.upsert(id, content);
    assertTrue(mutationResult.cas() != 0);

    GetResult getResult = collection.get(id, getOptions().project("foo", "created"));
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiration().isPresent());

    JsonObject decoded = getResult.contentAsObject();
    assertEquals("bar", decoded.getString("foo"));
    assertEquals(true, decoded.getBoolean("created"));
    assertFalse(decoded.containsKey("age"));
  }

  /**
   * Right now the mock does not support xattr/macro expansion so this test is
   * ignored on the mock. Once the mock supports it, please remove the ignore
   * annotation.
   *
   * <p>See https://github.com/couchbase/CouchbaseMock/issues/46</p>
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
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

    GetResult getResult = collection.get(id, getOptions().withExpiration(true));
    assertTrue(getResult.expiration().isPresent());
    assertTrue(getResult.expiration().get().toMillis() > 0);
    assertEquals(content, getResult.contentAsObject());
  }

  /**
   * Right now the mock does not support xattr/macro expansion so this test is
   * ignored on the mock. Once the mock supports it, please remove the ignore
   * annotation.
   *
   * <p>See https://github.com/couchbase/CouchbaseMock/issues/46</p>
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
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
      getOptions().project("foo", "created").withExpiration(true)
    );
    assertTrue(getResult.cas() != 0);
    assertTrue(getResult.expiration().isPresent());
    assertTrue(getResult.expiration().get().toMillis() > 0);

    JsonObject decoded = getResult.contentAsObject();
    assertEquals("bar", decoded.getString("foo"));
    assertEquals(true, decoded.getBoolean("created"));
    assertFalse(decoded.containsKey("age"));
  }

  @Test
  void failsIfOverMaxProjectionsInList() {
    assertThrows(UnsupportedOperationException.class, () ->
      collection.get("some_id", getOptions().project(
        "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "10", "11", "12", "13", "14", "15", "16", "17"
      ))
    );
  }

  /**
   * We need to ignore this test on the mock because the mock returns TMPFAIL instead of LOCKED when the
   * document is locked (which used to be the old functionality but now since XERROR is negotiated it
   * returns LOCKED properly).
   *
   * <p>Once the mock is modified to return LOCKED, this test can also be run on the mock again.</p>
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void getAndLock() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(id, expected);

    assertTrue(insert.cas() != 0);

    GetResult getAndLock = collection.getAndLock(id);

    assertTrue(getAndLock.cas() != 0);
    assertNotEquals(insert.cas(), getAndLock.cas());
    assertEquals(expected, getAndLock.contentAsObject());

    RequestTimeoutException exception = assertThrows(
      RequestTimeoutException.class,
      () -> collection.getAndLock(id, getAndLockOptions().timeout(Duration.ofMillis(100)))
    );
    assertEquals(EnumSet.of(RetryReason.KV_LOCKED), exception.requestContext().retryReasons());
    assertThrows(KeyNotFoundException.class, () -> collection.getAndLock("some_doc"));
  }

  /**
   * This test is ignored against the mock because right now it does not bump the CAS like
   * the server does when getAndTouch is called.
   *
   * <p>Remove the ignore as soon as https://github.com/couchbase/CouchbaseMock/issues/49 is
   * fixed.</p>
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
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
      } catch (KeyNotFoundException knf) {
        return true;
      }
    });
  }

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
      CASMismatchException.class,
      () -> collection.remove(id, removeOptions().cas(insert.cas() + 100))
    );

    MutationResult result = collection.remove(id);
    assertTrue(result.cas() != insert.cas());

    assertThrows(KeyNotFoundException.class, () -> collection.remove(id));
  }

  @Test
  void insert() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(id, expected);
    assertTrue(insert.cas() != 0);

    assertThrows(KeyExistsException.class, () -> collection.insert(id, expected));
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

  @Test
  void replace() {
    String id = UUID.randomUUID().toString();

    MutationResult upsert = collection.upsert(
      id,
      JsonObject.create().put("foo", true)
    );
    assertTrue(upsert.cas() != 0);

    assertThrows(CASMismatchException.class, () -> collection.replace(
      id,
      JsonObject.empty(),
      ReplaceOptions.replaceOptions().cas(upsert.cas() + 1))
    );

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult replaced = collection.replace(id, expected);
    assertTrue(replaced.cas() != 0);
    assertTrue(upsert.cas() != replaced.cas());

    assertEquals(expected, collection.get(id).contentAsObject());

    assertThrows(
      KeyNotFoundException.class,
      () -> collection.replace("some_doc", JsonObject.empty())
    );
  }

  /**
   * Right now the mock does not change the cas on touch, so we need to ignore the test
   * until https://github.com/couchbase/CouchbaseMock/issues/50 is resolved.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
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
      } catch (KeyNotFoundException knf) {
        return true;
      }
    });
  }

  /**
   * The mock returns TMPFAIL instead of LOCKED, so this test is ignored on the mock.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
  void unlock() {
    String id = UUID.randomUUID().toString();

    MutationResult upsert = collection.upsert(id, JsonObject.create().put("foo", true));
    assertTrue(upsert.cas() != 0);

    GetResult locked = collection.getAndLock(id);

    RequestTimeoutException exception = assertThrows(
      RequestTimeoutException.class,
      () -> collection.upsert(id, JsonObject.empty(), upsertOptions().timeout(Duration.ofMillis(100))));
    assertEquals(EnumSet.of(RetryReason.KV_LOCKED), exception.requestContext().retryReasons());

    exception = assertThrows(
      RequestTimeoutException.class,
      () -> collection.unlock(id, locked.cas() + 1, unlockOptions().timeout(Duration.ofMillis(100)))
    );
    assertEquals(EnumSet.of(RetryReason.KV_LOCKED), exception.requestContext().retryReasons());

    collection.unlock(id, locked.cas());

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult replaced = collection.replace(id, expected);
    assertTrue(replaced.cas() != 0);

    assertEquals(expected, collection.get(id).contentAsObject());
  }

  @Test
  void append() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

    assertThrows(
      KeyNotFoundException.class,
      () -> collection.binary().append(id, helloBytes)
    );

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id).contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.binary().append(id, worldBytes);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      helloWorldBytes,
      collection.get(id).contentAs(BinaryContent.class).content()
    );
  }

  @Test
  void appendReactive() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

    assertThrows(
            KeyNotFoundException.class,
            () -> collection.reactive().binary().append(id, helloBytes).block()
    );

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
            helloBytes,
            collection.get(id).contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.reactive().binary().append(id, worldBytes).block();
    assertNotNull(append);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
            helloWorldBytes,
            collection.get(id).contentAs(BinaryContent.class).content()
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

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
            helloBytes,
            collection.get(id).contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.async().binary().append(id, worldBytes).get();
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
            helloWorldBytes,
            collection.get(id).contentAs(BinaryContent.class).content()
    );
  }

  @Test
  void prepend() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] worldHelloBytes = "World!Hello, ".getBytes(UTF_8);

    assertThrows(
      KeyNotFoundException.class,
      () -> collection.binary().prepend(id, helloBytes)
    );

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id).contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.binary().prepend(id, worldBytes);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      worldHelloBytes,
      collection.get(id).contentAs(BinaryContent.class).content()
    );
  }

  @Test
  void increment() {
    String id = UUID.randomUUID().toString();

    assertThrows(KeyNotFoundException.class, () -> collection.binary().increment(id));

    CounterResult result = collection.binary().increment(
      id,
      incrementOptions().initial(Optional.of(5L))
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


  /**
   * Right now the mock allows the value to be decremented below zero, which is against the server
   * spec/protocol. Once https://github.com/couchbase/CouchbaseMock/issues/51 is fixed, this
   * ignore annotation can be removed.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
  void decrement() {
    String id = UUID.randomUUID().toString();

    assertThrows(KeyNotFoundException.class, () -> collection.binary().decrement(id));

    CounterResult result = collection.binary().decrement(
      id,
      decrementOptions().initial(Optional.of(2L))
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
}
