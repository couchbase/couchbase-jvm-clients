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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.core.msg.kv.UpsertResponse;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

/**
 * These integration tests verify synchronous replication capabilities against
 * a real cluster.
 *
 * <p>Note that for now the mock does not support sync replication.</p>
 */
class SyncReplicationIntegrationTest extends CoreIntegrationTest {

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
    core.openBucket(config().bucketname()).block();
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown();
  }

  /**
   * This test works if a cluster has two or more nodes and one replica configured.
   */
  @Test
  @IgnoreWhen(
    nodesLessThan = 2,
    replicasLessThan = 1,
    replicasGreaterThan = 1,
    missesCapabilities = { Capabilities.SYNC_REPLICATION }
  )
  void upsertSuccessfullyToMajority() throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    UpsertRequest upsertRequest = new UpsertRequest(id, content, 0, 0,
      Duration.ofSeconds(1), core.context(), CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(),
      Optional.of(DurabilityLevel.MAJORITY));
    core.send(upsertRequest);

    UpsertResponse upsertResponse = upsertRequest.response().get();
    assertTrue(upsertResponse.status().success());
    assertTrue(upsertResponse.cas() != 0);
  }

}
