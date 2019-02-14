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
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.RemoveResponse;
import com.couchbase.client.core.service.kv.Observe;
import com.couchbase.client.core.service.kv.ObserveContext;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObserveIntegrationTest extends CoreIntegrationTest {

  private static final Duration MAX_WAIT = Duration.ofSeconds(10);

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment()
      .ioConfig(IoConfig.mutationTokensEnabled(true))
      .build();
    core = Core.create(env);
    core.openBucket(config().bucketname()).block();
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown();
  }

  @Test
  void returnsIfBothAreNone() {
    String id = UUID.randomUUID().toString();
    InsertResponse insertResponse = performInsert(id);

    ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.NONE,
      Observe.ObserveReplicateTo.NONE, Optional.empty(), insertResponse.cas(), config().bucketname(),
      id, null, false, env.timeoutConfig().kvTimeout());

    Observe.poll(ctx).timeout(MAX_WAIT).block();
  }

  @Nested
  @DisplayName("Via CAS")
  class ObserveViaCas {

    @Test
    void persistToActive() {
      String id = UUID.randomUUID().toString();
      InsertResponse insertResponse = performInsert(id);

      ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, Optional.empty(), insertResponse.cas(), config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      Observe.poll(ctx).timeout(MAX_WAIT).block();
    }

    @Test
    void observesRemove() {
      String id = UUID.randomUUID().toString();

      InsertResponse insertResponse = performInsert(id);
      ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, Optional.empty(), insertResponse.cas(), config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());
      Observe.poll(ctx).timeout(MAX_WAIT).block();

      RemoveResponse removeResponse = performRemove(id);
      ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, Optional.empty(), removeResponse.cas(), config().bucketname(),
        id, null, true, env.timeoutConfig().kvTimeout());
      Observe.poll(ctx2).timeout(MAX_WAIT).block();
    }

    @Test
    @IgnoreWhen(replicasGreaterThan = 1)
    void failsFastIfTooManyReplicasRequested() {
      String id = UUID.randomUUID().toString();
      InsertResponse insertResponse = performInsert(id);

      final ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.THREE,
        Observe.ObserveReplicateTo.NONE, Optional.empty(), insertResponse.cas(), config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx).timeout(MAX_WAIT).block());

      final ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.NONE,
        Observe.ObserveReplicateTo.TWO, Optional.empty(), insertResponse.cas(), config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx2).timeout(MAX_WAIT).block());

      final ObserveContext ctx3 = new ObserveContext(core.context(), Observe.ObservePersistTo.FOUR,
        Observe.ObserveReplicateTo.THREE, Optional.empty(), insertResponse.cas(), config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx3).timeout(MAX_WAIT).block());
    }

    @Test
    void observesRemoveOnNotExistentDoc() {
      ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, Optional.empty(), 12345, config().bucketname(),
        "someNotExistentDoc", null, true, env.timeoutConfig().kvTimeout());
      Observe.poll(ctx2).timeout(MAX_WAIT).block();
    }

    @Test
    @IgnoreWhen(replicasLessThan = 1, nodesGreaterThan = 1)
    void timesOutIfReplicaNotAvailableWithBestEffort() {
      String id = UUID.randomUUID().toString();
      InsertResponse insertResponse = performInsert(id);

      Duration timeout = Duration.ofSeconds(1);
      ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.NONE,
        Observe.ObserveReplicateTo.ONE, Optional.empty(), insertResponse.cas(), config().bucketname(),
        id, null, false, timeout);

      try {
        Observe.poll(ctx).timeout(MAX_WAIT).block();
      } catch (Exception ex) {
          assertTrue(ex.getCause() instanceof TimeoutException);
      }
    }

  }

  @Nested
  @DisplayName("Via MutationToken")
  class ObserveViaMutationToken {

    @Test
    void persistToActive() {
      String id = UUID.randomUUID().toString();
      InsertResponse insertResponse = performInsert(id);
      assertTrue(insertResponse.mutationToken().isPresent());

      ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, insertResponse.mutationToken(), 0, config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      Observe.poll(ctx).timeout(MAX_WAIT).block();
    }

    @Test
    void observesRemove() {
      String id = UUID.randomUUID().toString();

      InsertResponse insertResponse = performInsert(id);
      assertTrue(insertResponse.mutationToken().isPresent());

      ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, insertResponse.mutationToken(), 0, config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());
      Observe.poll(ctx).timeout(MAX_WAIT).block();

      RemoveResponse removeResponse = performRemove(id);
      assertTrue(insertResponse.mutationToken().isPresent());

      ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.ACTIVE,
        Observe.ObserveReplicateTo.NONE, removeResponse.mutationToken(), 0, config().bucketname(),
        id, null, true, env.timeoutConfig().kvTimeout());
      Observe.poll(ctx2).timeout(MAX_WAIT).block();
    }

    @Test
    @IgnoreWhen(replicasGreaterThan = 1)
    void failsFastIfTooManyReplicasRequested() {
      String id = UUID.randomUUID().toString();
      InsertResponse insertResponse = performInsert(id);
      assertTrue(insertResponse.mutationToken().isPresent());

      final ObserveContext ctx = new ObserveContext(core.context(), Observe.ObservePersistTo.THREE,
        Observe.ObserveReplicateTo.NONE, insertResponse.mutationToken(), 0, config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx).timeout(MAX_WAIT).block());

      final ObserveContext ctx2 = new ObserveContext(core.context(), Observe.ObservePersistTo.NONE,
        Observe.ObserveReplicateTo.TWO, insertResponse.mutationToken(), 0, config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx2).timeout(MAX_WAIT).block());

      final ObserveContext ctx3 = new ObserveContext(core.context(), Observe.ObservePersistTo.FOUR,
        Observe.ObserveReplicateTo.THREE, insertResponse.mutationToken(), 0, config().bucketname(),
        id, null, false, env.timeoutConfig().kvTimeout());

      assertThrows(ReplicaNotConfiguredException.class, () -> Observe.poll(ctx3).timeout(MAX_WAIT).block());
    }

  }

  /**
   * Helper method to write a new document which can then be observed.
   *
   * @param id the document id.
   * @return returns the response to use for observe.
   */
  private InsertResponse performInsert(final String id) {
    byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
      env.timeoutConfig().kvTimeout(), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
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
    RemoveRequest removeRequest = new RemoveRequest(id, null, 0, env.timeoutConfig().kvTimeout(),
      core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
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
