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
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.config.loader.GlobalLoader;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@IgnoreWhen(missesCapabilities = Capabilities.GLOBAL_CONFIG)
class GlobalBucketRefresherIntegrationTest extends CoreIntegrationTest {

  private Core core;
  private CoreEnvironment env;

  @BeforeEach
  void beforeEach() {
    env = environment().build();
    core = Core.create(env, authenticator(), seedNodes());
  }

  @AfterEach
  void afterEach() {
    core.shutdown().block();
    env.shutdown();
  }

  /**
   * This test makes sure that configs can be fetched but start, stop and shutdown is also
   * honored.
   */
  @Test
  void refreshGlobalConfigViaCarrierPublication() throws Exception {
    GlobalConfigCountingConfigurationProvider provider = new GlobalConfigCountingConfigurationProvider(core);
    GlobalRefresher refresher = new GlobalRefresher(provider, core);

    // prime the config provider with a global config loaded
    loadGlobalConfig(provider);

    // The config pushed from the loader is also proposed initially
    assertEquals(1, provider.proposedConfigs.size());

    refresher.start().block();

    // Default config poll interval is 2.5 seconds, so give enough time for the two refresh iterations that this test
    // is looking for.
    int failSafety = 30;
    while (true) {
      failSafety--;
      Thread.sleep(500);

      if (provider.proposedConfigs.size() >= 3) {
        break;
      }

      if (failSafety <= 0) {
        fail("No bucket configs proposed in the allowed safety time window");
      }
    }

    refresher.stop().block();

    int currentSize = provider.proposedConfigs.size();
    Thread.sleep(2000);
    // Wait to make sure if stopped no more configs are proposed
    assertEquals(currentSize, provider.proposedConfigs.size());

    refresher.shutdown().block();
  }

  /**
   * Not related, but to test the refresh we first need to load the global config and prime the config provider.
   */
  private void loadGlobalConfig(final ConfigurationProvider provider) {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();
    GlobalLoader loader = new GlobalLoader(core);

    ProposedGlobalConfigContext globalConfigContext = loader.load(
      NodeIdentifier.forBootstrap(config.hostname(), config.ports().get(Services.MANAGER)),
      config.ports().get(Services.KV)
    ).block();

    provider.proposeGlobalConfig(globalConfigContext);
  }

  /**
   * Extends the config provider to allow for introspection on the proposed global configs.
   */
  static class GlobalConfigCountingConfigurationProvider extends DefaultConfigurationProvider {

    private final List<ProposedGlobalConfigContext> proposedConfigs;

    GlobalConfigCountingConfigurationProvider(final Core core) {
      super(core, asConnectionString(GlobalBucketRefresherIntegrationTest.seedNodes()));
      this.proposedConfigs = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void proposeGlobalConfig(ProposedGlobalConfigContext ctx) {
      proposedConfigs.add(ctx);
      super.proposeGlobalConfig(ctx);
    }

  }

}
