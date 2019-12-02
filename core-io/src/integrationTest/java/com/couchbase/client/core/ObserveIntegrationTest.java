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
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.ReplicaNotConfiguredException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.RemoveResponse;
import com.couchbase.client.core.service.kv.Observe;
import com.couchbase.client.core.service.kv.ObserveContext;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObserveIntegrationTest extends CoreIntegrationTest {

  private static final Duration MAX_WAIT = Duration.ofSeconds(10);

  private static Core core;
  private static CoreEnvironment env;
  private static CollectionIdentifier cid;

  @BeforeAll
  static void beforeAll() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname());
    cid = CollectionIdentifier.fromDefault(config().bucketname());

    waitUntilCondition(() -> core.clusterConfig().hasClusterOrBucketConfig());
  }

  @AfterAll
  static void afterAll() {
    core.shutdown().block();
    env.shutdown();
  }

  @Test
  void returnsIfBothAreNone() {
    String id = UUID.randomUUID().toString();
    InsertResponse insertResponse = performInsert(id);

    ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.NONE,
      Observe.ObserveReplicateTo.NONE, Optional.empty(), insertResponse.cas(), cid,
      id, false, env.timeoutConfig().kvTimeout(), null);

    Observe.poll(ctx).timeout(MAX_WAIT).block();
  }

  @Test
  void persistToActive() {
    String id = UUID.randomUUID().toString();
    InsertResponse insertResponse = performInsert(id);
    assertTrue(insertResponse.mutationToken().isPresent());

    ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
      Observe.ObserveReplicateTo.NONE, insertResponse.mutationToken(), 0, cid,
      id, false, env.timeoutConfig().kvTimeout(), null);

    Observe.poll(ctx).timeout(MAX_WAIT).block();
  }

  @Test
  void observesRemove() {
    String id = UUID.randomUUID().toString();

    InsertResponse insertResponse = performInsert(id);
    assertTrue(insertResponse.mutationToken().isPresent());

    ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
      Observe.ObserveReplicateTo.NONE, insertResponse.mutationToken(), 0, cid,
      id, false, env.timeoutConfig().kvTimeout(), null);
    Observe.poll(ctx).timeout(MAX_WAIT).block();

    RemoveResponse removeResponse = performRemove(id);
    assertTrue(insertResponse.mutationToken().isPresent());

    ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
      Observe.ObserveReplicateTo.NONE, removeResponse.mutationToken(), 0, cid,
      id, true, env.timeoutConfig().kvTimeout(), null);
    Observe.poll(ctx2).timeout(MAX_WAIT).block();
  }

  @Test
  @IgnoreWhen(replicasGreaterThan = 1)
  void failsFastIfTooManyReplicasRequested() {
    String id = UUID.randomUUID().toString();
    InsertResponse insertResponse = performInsert(id);
    assertTrue(insertResponse.mutationToken().isPresent());

    final ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.THREE,
      Observe.ObserveReplicateTo.NONE, insertResponse.mutationToken(), 0, cid,
      id, false, env.timeoutConfig().kvTimeout(), null);

    assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx).timeout(MAX_WAIT).block());

    final ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.NONE,
      Observe.ObserveReplicateTo.TWO, insertResponse.mutationToken(), 0, cid,
      id, false, env.timeoutConfig().kvTimeout(), null);

    assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx2).timeout(MAX_WAIT).block());

    final ObserveContext ctx3 = new ObserveContext(core.context(), Observe.ObservePersistTo.FOUR,
      Observe.ObserveReplicateTo.THREE, insertResponse.mutationToken(), 0, cid,
      id, false, env.timeoutConfig().kvTimeout(), null);

    assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx3).timeout(MAX_WAIT).block());
  }
  /**
   * Helper method to write a new document which can then be observed.
   *
   * @param id the document id.
   * @return returns the response to use for observe.
   */
  private InsertResponse performInsert(final String id) {
    byte[] content = "hello, world".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      env.timeoutConfig().kvTimeout(), core.context(), cid, env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    InsertResponse insertResponse;
    try {
      insertResponse = insertRequest.response().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertTrue(insertResponse.status().success());
    assertTrue(insertResponse.cas() != 0);
    return insertResponse;
  }

  /**
   * Helper method to perform a remove operation that can then be observed.
   *
   * @param id the document id.
   * @return returns the response to use for observe.
   */
  private RemoveResponse performRemove(final String id) {
    RemoveRequest removeRequest = new RemoveRequest(id, 0, env.timeoutConfig().kvTimeout(),
      core.context(), cid, env.retryStrategy(), Optional.empty(), null);
    core.send(removeRequest);

    RemoveResponse removeResponse;
    try {
      removeResponse = removeRequest.response().get();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
    assertTrue(removeResponse.status().success());
    assertTrue(removeResponse.cas() != 0);
    return removeResponse;
  }

}
