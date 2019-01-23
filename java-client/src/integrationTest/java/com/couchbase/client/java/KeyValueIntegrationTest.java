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
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    environment.shutdown(Duration.ofSeconds(1));
    cluster.shutdown();
  }

  @Test
  void insertAndGet() {
    String id = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(id, "Hello, World");

    assertTrue(insertResult.cas() != 0);
    assertFalse(insertResult.mutationToken().isPresent());

    GetResult getResult = collection.get(id).get();
    assertEquals(id, getResult.id());
    assertEquals("Hello, World", getResult.contentAs(String.class));
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiration().isPresent());
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

    assertEquals(id, existsResult.get().id());
    assertEquals(insertResult.cas(), existsResult.get().cas());
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
      assertEquals(id, r.id());
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
      assertEquals(id, r.id());
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
    assertThrows(IllegalArgumentException.class, () ->
      collection.get("some_id", getOptions().project(
        "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "10", "11", "12", "13", "14", "15", "16", "17"
      ))
    );
  }

  @Test
  void getAndLock() {

  }

  @Test
  void getAndTouch() {

  }

}
