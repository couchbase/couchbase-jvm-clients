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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies the functionality of the {@link ClusterManagerLoader}.
 */
class ClusterManagerLoaderIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }

  /**
   * This is a very simplistic test that makes sure that we can "round trip" in the
   * {@link ClusterManagerLoader} by grabbing a JSON decodable config through the full stack.
   */
  @Test
  void loadConfigViaClusterManagerHttp() {
    TestNodeConfig config = config().firstNodeWith(Services.MANAGER).get();

    Core core = Core.create(env);
    ClusterManagerLoader loader = new ClusterManagerLoader(core);
    BucketConfig loaded = loader.load(
      NetworkAddress.create(config.hostname()),
      config.ports().get(Services.MANAGER),
      config().bucketname()
    ).block();

    assertNotNull(loaded);
    assertEquals(config().bucketname(), loaded.name());

    core.shutdown().block();
  }

}
