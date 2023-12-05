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
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.util.ConfigWaitHelper;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies the functionality of the {@link KeyValueBucketLoader}.
 */
class KeyValueBucketLoaderIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;
  private ConfigWaitHelper configWaitHelper;

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
   * {@link KeyValueBucketLoader} by grabbing a JSON decodable config through the full stack.
   */
  @Test
  void loadConfigViaCarrierPublication() {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    Core core = Core.create(env, authenticator(), seedNodes());
    configWaitHelper.await();
    KeyValueBucketLoader loader = new KeyValueBucketLoader(core);
    ProposedBucketConfigContext loaded = loader.load(
      new NodeIdentifier(config.hostname(), config.ports().get(Services.MANAGER)),
      config.ports().get(Services.KV),
      config().bucketname(),
      Optional.empty()
    ).block();

    assertNotNull(loaded);
    assertEquals(config().bucketname(), loaded.bucketName());

    core.shutdown().block();
  }

}
