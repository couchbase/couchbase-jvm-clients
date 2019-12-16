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
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.java.kv.DecrementOptions.decrementOptions;
import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
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
  static private Collection collection;

  @BeforeAll
  static void beforeAll() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    cluster.waitUntilReady(Duration.ofSeconds(5));
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
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiry().isPresent());
  }

  /**
   * Mock does not support Get Meta, so we need to ignore it there.
   */
  @Test
  @IgnoreWhen( clusterTypes = ClusterType.MOCKED )
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
    assertFalse(getResult.expiry().isPresent());

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

    GetResult getResult = collection.get(id, getOptions().withExpiry(true));
    assertTrue(getResult.expiry().isPresent());
    assertTrue(getResult.expiry().get().toMillis() > 0);
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
      getOptions().project("foo", "created").withExpiry(true)
    );
    assertTrue(getResult.cas() != 0);
    assertTrue(getResult.expiry().isPresent());
    assertTrue(getResult.expiry().get().toMillis() > 0);

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
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
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
      } catch (DocumentNotFoundException knf) {
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
      CasMismatchException.class,
      () -> collection.remove(id, removeOptions().cas(insert.cas() + 100))
    );

    MutationResult result = collection.remove(id);
    assertTrue(result.cas() != insert.cas());

    assertThrows(DocumentNotFoundException.class, () -> collection.remove(id));
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
      JsonObject.empty(),
      ReplaceOptions.replaceOptions().cas(upsert.cas() + 1))
    );

    JsonObject expected = JsonObject.create().put("foo", false);
    MutationResult replaced = collection.replace(id, expected);
    assertTrue(replaced.cas() != 0);
    assertTrue(upsert.cas() != replaced.cas());

    assertEquals(expected, collection.get(id).contentAsObject());

    assertThrows(
      DocumentNotFoundException.class,
      () -> collection.replace("some_doc", JsonObject.empty())
    );
  }

  /**
   *  It seems the mock will not actually upsert a doc when the expiry exceeds 30 days.
   *  So https://github.com/couchbase/CouchbaseMock/issues/58 needs to be fixed, at
   *  which time we can remove the restriction on not running this test when mocked.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
  void checkExpiryBeyond2038() {
    String id = UUID.randomUUID().toString();
    JsonObject obj = JsonObject.create().put("foo", true);
    // well...  to get past 2038, it is now 2019.  So there are about
    // 60 * 60 * 24 * 360 seconds in a year, and so if we move 30 years
    // into the future that seems reasonable.  The server claims to use
    // and unsigned int, which should give 40 years of seconds.


    // 30 years in the future...
    LocalDate future = LocalDate.now().plusYears(30);
    Duration futureDuration = Duration.ofSeconds(future.atStartOfDay().toEpochSecond(ZoneOffset.UTC));

    // make sure we are not insane.
    assertTrue(future.getYear() > 2038);

    collection.upsert(id, obj, UpsertOptions.upsertOptions().expiry(futureDuration));

    GetResult result = collection.get(id, GetOptions.getOptions().withExpiry(true));
    // so lets not calculate it exactly, but 30 years from now should be more
    // than 360 * 30...
    LocalDateTime expiry = LocalDateTime.ofEpochSecond(result.expiry().get().getSeconds(), 0, ZoneOffset.UTC);
    assertEquals(future.getYear(), expiry.getYear());
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
      } catch (DocumentNotFoundException knf) {
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

    GetResult locked = collection.getAndLock(id, Duration.ofSeconds(30));

    TimeoutException exception = assertThrows(
      TimeoutException.class,
      () -> collection.upsert(id, JsonObject.empty(), upsertOptions().timeout(Duration.ofMillis(100))));
    assertEquals(EnumSet.of(RetryReason.KV_LOCKED), exception.context().requestContext().retryReasons());

    assertThrows(CasMismatchException.class, () -> collection.unlock(id, locked.cas() + 1));

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


  /**
   * Right now the mock allows the value to be decremented below zero, which is against the server
   * spec/protocol. Once https://github.com/couchbase/CouchbaseMock/issues/51 is fixed, this
   * ignore annotation can be removed.
   */
  @Test
  @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
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

  @Test
  void throwsIfTooLarge() {
    String id = UUID.randomUUID().toString();
    JsonObject content = JsonObject.empty();
    for (int i = 0; i < 400000; i++) {
      content.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    assertThrows(ValueTooLargeException.class, () -> collection.upsert(id, content));
  }

}
