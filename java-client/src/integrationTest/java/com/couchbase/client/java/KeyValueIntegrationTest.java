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
import com.couchbase.client.core.error.DocumentAlreadyExistsException;
import com.couchbase.client.core.error.DocumentDoesNotExistException;
import com.couchbase.client.core.error.TemporaryLockFailureException;
import com.couchbase.client.java.codec.BinaryContent;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.java.kv.DecrementOptions.decrementOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.IncrementOptions.incrementOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    cluster.shutdown();
    environment.shutdown();
  }

  @Test
  void insertAndGet() {
    String id = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(id, "Hello, World");

    assertTrue(insertResult.cas() != 0);
    assertFalse(insertResult.mutationToken().isPresent());

    Optional<GetResult> getResult = collection.get(id);
    assertTrue(getResult.isPresent());
    getResult.ifPresent(r -> {
      assertEquals("Hello, World", r.contentAs(String.class));
      assertTrue(r.cas() != 0);
      assertFalse(r.expiration().isPresent());
    });
  }

  @Test
  void exists() {
    String id = UUID.randomUUID().toString();

    Optional<ExistsResult> existsResult = collection.exists(id);
    assertFalse(existsResult.isPresent());

    MutationResult insertResult = collection.insert(id, "Hello, World");
    assertTrue(insertResult.cas() != 0);

    existsResult = collection.exists(id);
    assertTrue(existsResult.isPresent());

    assertEquals(insertResult.cas(), existsResult.get().cas());

    assertFalse(collection.exists("some_id").isPresent());
  }

  @Test
  void emptyIfGetNotFound() {
    assertFalse(collection.get(UUID.randomUUID().toString()).isPresent());
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

    Optional<GetResult> getResult = collection.get(id, getOptions().project("foo", "created"));
    assertTrue(getResult.isPresent());
    getResult.ifPresent(r -> {
      assertTrue(r.cas() != 0);
      assertFalse(r.expiration().isPresent());

      JsonObject decoded = r.contentAsObject();
      assertEquals("bar", decoded.getString("foo"));
      assertEquals(true, decoded.getBoolean("created"));
      assertFalse(decoded.containsKey("age"));
    });
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

    Optional<GetResult> getResult = collection.get(id, getOptions().withExpiration(true));
    assertTrue(getResult.isPresent());
    getResult.ifPresent(r -> {
      assertTrue(r.expiration().isPresent());
      assertTrue(r.expiration().get().toMillis() > 0);
      assertEquals(content, r.contentAsObject());
    });
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

    Optional<GetResult> getResult = collection.get(
      id,
      getOptions().project("foo", "created").withExpiration(true)
    );
    assertTrue(getResult.isPresent());
    getResult.ifPresent(r -> {
      assertTrue(r.cas() != 0);
      assertTrue(r.expiration().isPresent());
      assertTrue(r.expiration().get().toMillis() > 0);

      JsonObject decoded = r.contentAsObject();
      assertEquals("bar", decoded.getString("foo"));
      assertEquals(true, decoded.getBoolean("created"));
      assertFalse(decoded.containsKey("age"));
    });
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

  @Test
  void getAndLock() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(id, expected);

    assertTrue(insert.cas() != 0);

    Optional<GetResult> getAndLock = collection.getAndLock(id);

    assertTrue(getAndLock.isPresent());
    assertTrue(getAndLock.get().cas() != 0);
    assertNotEquals(insert.cas(), getAndLock.get().cas());
    assertEquals(expected, getAndLock.get().contentAsObject());

    assertThrows(TemporaryLockFailureException.class, () -> collection.getAndLock(id));
    assertFalse(collection.getAndLock("some_doc").isPresent());
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

    Optional<GetResult> getAndTouch = collection.getAndTouch(id, Duration.ofSeconds(1));

    assertTrue(getAndTouch.isPresent());
    assertTrue(getAndTouch.get().cas() != 0);
    assertNotEquals(insert.cas(), getAndTouch.get().cas());
    assertEquals(expected, getAndTouch.get().contentAsObject());

    waitUntilCondition(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // ignored.
      }
      return !collection.get(id).isPresent();
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

    assertThrows(DocumentDoesNotExistException.class, () -> collection.remove(id));
  }

  @Test
  void insert() {
    String id = UUID.randomUUID().toString();

    JsonObject expected = JsonObject.create().put("foo", true);
    MutationResult insert = collection.insert(id, expected);
    assertTrue(insert.cas() != 0);

    assertThrows(DocumentAlreadyExistsException.class, () -> collection.insert(id, expected));
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

    assertEquals(expected, collection.get(id).get().contentAsObject());
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

    assertEquals(expected, collection.get(id).get().contentAsObject());

    assertThrows(
      DocumentDoesNotExistException.class,
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

    Optional<GetResult> get = collection.get(id);
    assertTrue(get.isPresent());
    get.ifPresent(r -> {
      assertEquals(expected, r.contentAsObject());
      assertEquals(r.cas(), touched.cas());
    });

    waitUntilCondition(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // ignored.
      }
      return !collection.get(id).isPresent();
    });
  }

  @Test
  void unlock() {
    String id = UUID.randomUUID().toString();

    MutationResult upsert = collection.upsert(id, JsonObject.create().put("foo", true));
    assertTrue(upsert.cas() != 0);

    Optional<GetResult> locked = collection.getAndLock(id);
    assertTrue(locked.isPresent());

    assertThrows(TemporaryLockFailureException.class, () -> collection.upsert(id, JsonObject.empty()));
    assertThrows(TemporaryLockFailureException.class, () -> collection.unlock(id, locked.get().cas() + 1));

    collection.unlock(id, locked.get().cas());

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult replaced = collection.replace(id, expected);
    assertTrue(replaced.cas() != 0);

    assertEquals(expected, collection.get(id).get().contentAsObject());
  }

  @Test
  void append() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

    assertThrows(
      DocumentDoesNotExistException.class,
      () -> collection.binary().append(id, helloBytes)
    );

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id).get().contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.binary().append(id, worldBytes);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      helloWorldBytes,
      collection.get(id).get().contentAs(BinaryContent.class).content()
    );
  }

  @Test
  void appendReactive() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] helloWorldBytes = "Hello, World!".getBytes(UTF_8);

    assertThrows(
            DocumentDoesNotExistException.class,
            () -> collection.reactive().binary().append(id, helloBytes).block()
    );

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
            helloBytes,
            collection.get(id).get().contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.reactive().binary().append(id, worldBytes).block();
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
            helloWorldBytes,
            collection.get(id).get().contentAs(BinaryContent.class).content()
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
            collection.get(id).get().contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.async().binary().append(id, worldBytes).get();
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
            helloWorldBytes,
            collection.get(id).get().contentAs(BinaryContent.class).content()
    );
  }

  @Test
  void prepend() {
    String id = UUID.randomUUID().toString();

    byte[] helloBytes = "Hello, ".getBytes(UTF_8);
    byte[] worldBytes = "World!".getBytes(UTF_8);
    byte[] worldHelloBytes = "World!Hello, ".getBytes(UTF_8);

    assertThrows(
      DocumentDoesNotExistException.class,
      () -> collection.binary().prepend(id, helloBytes)
    );

    MutationResult upsert = collection.upsert(id, BinaryContent.wrap(helloBytes));
    assertTrue(upsert.cas() != 0);
    assertArrayEquals(
      helloBytes,
      collection.get(id).get().contentAs(BinaryContent.class).content()
    );

    MutationResult append = collection.binary().prepend(id, worldBytes);
    assertTrue(append.cas() != 0);
    assertNotEquals(append.cas(), upsert.cas());

    assertArrayEquals(
      worldHelloBytes,
      collection.get(id).get().contentAs(BinaryContent.class).content()
    );
  }

  @Test
  void increment() {
    String id = UUID.randomUUID().toString();

    assertThrows(DocumentDoesNotExistException.class, () -> collection.binary().increment(id));

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

    assertThrows(DocumentDoesNotExistException.class, () -> collection.binary().decrement(id));

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
