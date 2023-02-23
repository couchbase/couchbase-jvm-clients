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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConfigWaitHelper;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies the functionality of the {@link ClusterManagerBucketLoader}.
 */
class ClusterManagerBucketLoaderIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;
  private static ConfigWaitHelper configWaitHelper;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
    configWaitHelper = new ConfigWaitHelper(env.eventBus());
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }

  /**
   * This is a very simplistic test that makes sure that we can "round trip" in the
   * {@link ClusterManagerBucketLoader} by grabbing a JSON decodable config through the full stack.
   */
  @Test
  void loadConfigViaClusterManagerHttp() throws Exception {
    TestNodeConfig config = config().firstNodeWith(Services.MANAGER).orElse(null);
    assertNotNull(config);

    Core core = Core.create(env, authenticator(), seedNodes());
    core.waitUntilReady(
      setOf(ServiceType.MANAGER, ServiceType.KV),
      Duration.ofSeconds(10),
      ClusterState.ONLINE,
      null
    ).get();
    configWaitHelper.await();
    ClusterManagerBucketLoader loader = new ClusterManagerBucketLoader(core);
    int port = config.ports().get(Services.MANAGER);
    ProposedBucketConfigContext ctx = loader.load(
      new NodeIdentifier(config.hostname(), port),
      port,
      config().bucketname(),
      Optional.empty()
    ).block();
    assertNotNull(ctx);

    BucketConfig loaded = BucketConfigParser.parse(ctx.config(), env, ctx.origin());
    assertNotNull(loaded);
    assertEquals(config().bucketname(), loaded.name());

    core.shutdown().block();
  }

}
