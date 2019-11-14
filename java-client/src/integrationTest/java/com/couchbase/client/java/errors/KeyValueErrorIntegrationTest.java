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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestTimeoutException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetAndTouchOptions.getAndTouchOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
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
    cluster = Cluster.connect(connectionString(), clusterOptions());
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

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
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void verifyGetAndLockDoubleLock() {
    String validId = UUID.randomUUID().toString();
    collection.upsert(validId, JsonObject.empty());
    collection.getAndLock(validId, Duration.ofSeconds(5));
    RequestTimeoutException exception = assertThrows(
      RequestTimeoutException.class,
      () -> collection.getAndLock(validId, Duration.ofSeconds(5), getAndLockOptions().timeout(Duration.ofSeconds(1)))
    );
    assertTrue(exception.context().requestContext().retryReasons().contains(RetryReason.KV_LOCKED));
  }

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
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void verifyTouchingLocked() {
    String validId = UUID.randomUUID().toString();
    collection.upsert(validId, JsonObject.empty());
    collection.getAndLock(validId, Duration.ofSeconds(5));
    RequestTimeoutException exception = assertThrows(
      RequestTimeoutException.class,
      () -> collection.getAndTouch(validId, Duration.ofSeconds(2), getAndTouchOptions().timeout(Duration.ofSeconds(1)))
    );
    assertTrue(exception.context().requestContext().retryReasons().contains(RetryReason.KV_LOCKED));
  }

  @Test
  void verifyExistsExceptions() {
    assertThrows(InvalidArgumentException.class, () -> collection.exists("foo", null));
    assertThrows(InvalidArgumentException.class, () -> collection.exists(null));
  }

}
