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

package com.couchbase.client.core.config;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.util.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link DefaultConfigurationProvider}.
 */
class DefaultConfigurationProviderTest {

  private static CoreEnvironment ENVIRONMENT;

  private static String ORIGIN = "127.0.0.1";

  @BeforeAll
  static void setup() {
    ENVIRONMENT = CoreEnvironment.create("user", "pass");
  }

  @AfterAll
  static void teardown() {
    ENVIRONMENT.shutdown();
  }

  @Test
  void canProposeNewBucketConfig() {
    Core core = mock(Core.class);
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs().subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, provider.seedNodes().size());

    String bucket = "default";
    String config = Utils.readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());
    assertFalse(provider.config().bucketConfigs().isEmpty());
    assertEquals(1073, provider.config().bucketConfig("default").rev());

    assertEquals(3, provider.seedNodes().size());
    for (SeedNode node : provider.seedNodes()) {
      assertEquals(11210, node.kvPort().get());
      assertEquals(8091, node.httpPort().get());
    }
  }

  @Test
  void ignoreProposedConfigWithLowerOrEqualRev() {
    Core core = mock(Core.class);
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs().subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());

    String bucket = "default";
    String config = Utils.readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());

    assertFalse(provider.config().bucketConfigs().isEmpty());
    assertEquals(1073, provider.config().bucketConfig("default").rev());
  }

  @Test
  void canUpdateConfigWithNewRev() {
    Core core = mock(Core.class);
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs().subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());

    String bucket = "default";
    String config = Utils.readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());

    String newConfig = Utils.readResource(
      "config_with_external_higher_rev.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, newConfig, ORIGIN));
    assertEquals(2, configsPushed.get());

    assertFalse(provider.config().bucketConfigs().isEmpty());
    assertEquals(1888, provider.config().bucketConfig("default").rev());
  }

  @Test
  void ignoreProposedConfigOnceShutdown() {
    Core core = mock(Core.class);
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs().subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());

    String bucket = "default";
    String config = Utils.readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());

    String newConfig = Utils.readResource(
      "config_with_external_higher_rev.json",
      DefaultConfigurationProviderTest.class
    );

    provider.shutdown().block();

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, newConfig, ORIGIN));
    assertEquals(3, configsPushed.get());

    assertTrue(provider.config().bucketConfigs().isEmpty());
  }

  /**
   * Makes sure that even if we only get a global config the seed nodes are properly updated from
   * that config.
   */
  @Test
  void updatesSeedNodesFromGlobalConfig() {
    Core core = mock(Core.class);
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core);

    String newConfig = Utils.readResource(
      "global_config_mad_hatter_multi_node.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeGlobalConfig(new ProposedGlobalConfigContext(newConfig, "127.0.0.1"));

    assertEquals(2, provider.seedNodes().size());
    for (SeedNode sn : provider.seedNodes()) {
      assertEquals(11210, sn.kvPort().get());
      assertEquals(8091, sn.httpPort().get());
      assertTrue(sn.address().equals("10.143.193.101") || sn.address().equals("10.143.193.102"));
    }
  }

}