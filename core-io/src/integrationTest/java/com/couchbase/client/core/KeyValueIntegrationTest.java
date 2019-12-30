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

package com.couchbase.client.core;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeyValueIntegrationTest extends CoreIntegrationTest {

  private static Core core;
  private static CoreEnvironment env;

  @BeforeAll
  static void beforeAll() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname());
  }

  @AfterAll
  static void afterAll() {
    core.shutdown().block();
    env.shutdown();
  }

  /**
   * Validate that an inserted document can be read subsequently.
   */
  @Test
  void insertAndGet() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      Duration.ofSeconds(1), core.context(), CollectionIdentifier.fromDefault(config().bucketname()),
      env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());

    GetRequest getRequest = new GetRequest(id, Duration.ofSeconds(1), core.context(),
      CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(), null);
    core.send(getRequest);

    GetResponse getResponse = getRequest.response().get();
    assertTrue(getResponse.status().success());
    assertArrayEquals(content, getResponse.content());
    assertTrue(getResponse.cas() != 0);
  }

  @Test
  @IgnoreWhen(hasCapabilities = { Capabilities.SYNC_REPLICATION })
  void failFastIfSyncReplicationNotAvailable() {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0, Duration.ofSeconds(1),
      core.context(), CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(),
      Optional.of(DurabilityLevel.MAJORITY), null);
    core.send(insertRequest);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> insertRequest.response().get());
    assertTrue(exception.getCause() instanceof FeatureNotAvailableException);
  }

  /**
   * The timer wheel has a resolution if 100ms by default, so very low timeouts might go through and never have
   * a chance of getting into the next tick.
   *
   * <p>The code has additional checks in place to proactively check for such a timeout. This test makes sure that
   * super low timeouts always hit.</p>
   */
  @Test
  void timesOutVeryLowTimeoutDurations() {
    GetRequest getRequest = new GetRequest("foo", Duration.ofNanos(1), core.context(),
      CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(), null);
    core.send(getRequest);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> getRequest.response().get());
    assertTrue(exception.getCause() instanceof TimeoutException);
  }

  @Test
  @IgnoreWhen(hasCapabilities = { Capabilities.COLLECTIONS })
  void shortCircuitCollectionsIfNotAvailable() {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      Duration.ofSeconds(5), core.context(), new CollectionIdentifier(
        config().bucketname(),
        Optional.of(CollectionIdentifier.DEFAULT_SCOPE),
        Optional.of("my_collection_name")
      ),
      env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> insertRequest.response().get());
    assertTrue(exception.getCause() instanceof FeatureNotAvailableException);

    InsertRequest insertRequest2 = new InsertRequest(id, content, 0, 0,
      Duration.ofSeconds(5), core.context(), new CollectionIdentifier(
      config().bucketname(),
      Optional.of("my_custom_scope"),
      Optional.of(CollectionIdentifier.DEFAULT_COLLECTION)
    ),
      env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest2);

    exception = assertThrows(ExecutionException.class, () -> insertRequest2.response().get());
    assertTrue(exception.getCause() instanceof FeatureNotAvailableException);

  }

}
