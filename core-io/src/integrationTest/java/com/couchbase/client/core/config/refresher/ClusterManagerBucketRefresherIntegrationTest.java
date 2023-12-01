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

package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.util.CoreIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Makes sure that the cluster manager refresher streams new configurations from the cluster.
 */
class ClusterManagerBucketRefresherIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;

  /**
   * We are using a shorter config poll interval in this test to keep the test runtime
   * small.
   */
  @BeforeEach
  void beforeEach() {
    env = environment().build();
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }

  @Test
  void streamsNewConfigurations() {
    Core core = Core.create(env, authenticator(), seedNodes());

    ProposedBucketConfigInspectingProvider inspectingProvider = new ProposedBucketConfigInspectingProvider(core.configurationProvider());
    ClusterManagerBucketRefresher refresher = new ClusterManagerBucketRefresher(inspectingProvider, core);

    core.openBucket(config().bucketname());

    waitUntilCondition(() -> core.clusterConfig().hasClusterOrBucketConfig());

    refresher.register(config().bucketname()).block();

    waitUntilCondition(() -> !inspectingProvider.proposedConfigs().isEmpty());

    ProposedBucketConfigContext proposed = inspectingProvider.proposedConfigs().get(0).proposedConfig();
    assertEquals(config().bucketname(), proposed.bucketName());
    assertNotNull(proposed.config());

    refresher.shutdown().block();
    core.shutdown().block();
  }

}
