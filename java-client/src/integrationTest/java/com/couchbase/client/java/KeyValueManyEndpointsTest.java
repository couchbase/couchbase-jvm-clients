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

package com.couchbase.client.java;

import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class KeyValueManyEndpointsTest extends JavaIntegrationTest {

  private Cluster cluster;
  private Collection collection;

  private static final int CONNS_TO_OPEN = 8;

  @BeforeEach
  void beforeEach() {
    cluster = createCluster(env -> env.ioConfig(IoConfig.numKvConnections(CONNS_TO_OPEN)));
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterEach
  void afterEach() {
    cluster.disconnect();
  }

  /**
   * This test makes sure that even when many kv endpoints per node are opened, all kinds of request go through
   * since they are distributed across them.
   *
   * <p>Since the index is built by looking at the partition of the key, we iterate through many IDs to catch any
   * out of bounds problems.</p>
   */
  @Test
  void usesAllEndpointsPerNode() {
    for (int i = 0; i < 2048; i++) {
      collection.upsert("id-" + i, "foobar");
      assertEquals("foobar", collection.get("id-" + i).contentAs(String.class));
    }
  }

  /**
   * Even though we open more than one kv endpoint per config, we need to make sure that we only open one
   * gcccp endpoint per node.
   */
  @IgnoreWhen(missesCapabilities = {Capabilities.GLOBAL_CONFIG})
  @Test
  void onlyOpensOneGcccpPerNode() {
    int bucket = 0;
    int global = 0;
    for (EndpointDiagnostics ed : cluster.diagnostics().endpoints().get(ServiceType.KV)) {
      if (ed.namespace().isPresent()) {
        bucket++;
      } else {
        global++;
      }
    }

    assertEquals(CONNS_TO_OPEN, bucket / global);
  }

}
