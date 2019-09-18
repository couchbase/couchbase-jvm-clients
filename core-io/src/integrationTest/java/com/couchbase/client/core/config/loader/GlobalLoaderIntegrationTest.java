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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@IgnoreWhen(missesCapabilities = Capabilities.GLOBAL_CONFIG)
class GlobalLoaderIntegrationTest extends CoreIntegrationTest {

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
   * {@link GlobalLoader} by grabbing a JSON decodable config through the full stack.
   */
  @Test
  void loadGlobalConfigViaCarrierPublication() {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    Core core = Core.create(env, authenticator(), seedNodes());
    GlobalLoader loader = new GlobalLoader(core);

    ProposedGlobalConfigContext globalConfigContext = loader.load(
      new NodeIdentifier(config.hostname(), config.ports().get(Services.MANAGER)),
      config.ports().get(Services.KV)
    ).block();

    assertNotNull(globalConfigContext);
    assertNotNull(globalConfigContext.config());

    core.shutdown().block();
  }

}