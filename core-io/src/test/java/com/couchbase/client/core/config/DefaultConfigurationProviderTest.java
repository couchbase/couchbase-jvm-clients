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
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.test.Util.readResource;
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

  private static final String ORIGIN = "127.0.0.1";

  @BeforeAll
  static void setup() {
    ENVIRONMENT = CoreEnvironment.create();
  }

  @AfterAll
  static void teardown() {
    ENVIRONMENT.shutdown();
  }

  @Test
  void currentConfigIsReplayedToLateSubscriber() {
    Core core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, SeedNode.LOCALHOST);
    provider.configs().blockFirst(Duration.ofSeconds(10));
  }

  @Test
  void canProposeNewBucketConfig() {
    Core core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, SeedNode.LOCALHOST);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs()
        .skip(1) // ignore initial empty config
        .subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, provider.seedNodes().size());

    String bucket = "default";
    String config = readResource(
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
      assertEquals(8091, node.clusterManagerPort().get());
    }

    assertEquals(Optional.empty(), ctx.alternateAddress());
  }

  @Test
  void ignoreProposedConfigWithLowerOrEqualRev() {
    Core core = mock(Core.class);
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class)));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, SeedNode.LOCALHOST);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs()
        .skip(1) // ignore initial empty config
        .subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());

    String bucket = "default";
    String config = readResource(
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
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class)));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, SeedNode.LOCALHOST);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs()
        .skip(1) // ignore initial empty config
        .subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());

    String newConfig = readResource(
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
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class)));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, SeedNode.LOCALHOST);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs()
        .skip(1) // ignore initial empty config
        .subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());

    String newConfig = readResource(
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
    when(core.context()).thenReturn(new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class)));

    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, SeedNode.LOCALHOST);

    String newConfig = readResource(
      "global_config_mad_hatter_multi_node.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeGlobalConfig(new ProposedGlobalConfigContext(newConfig, "127.0.0.1"));

    assertEquals(2, provider.seedNodes().size());
    for (SeedNode sn : provider.seedNodes()) {
      assertEquals(11210, sn.kvPort().get());
      assertEquals(8091, sn.clusterManagerPort().get());
      assertTrue(sn.address().equals("10.143.193.101") || sn.address().equals("10.143.193.102"));
    }
  }

  @Test
  void externalModeSelectedIfAuto() {
    Core core = mock(Core.class);
    CoreEnvironment environment = CoreEnvironment.create();

    CoreContext ctx = new CoreContext(core, 1, environment, PasswordAuthenticator.create("user", "pw"));
    when(core.context()).thenReturn(ctx);

    Set<SeedNode> seedNodes = new HashSet<>(Collections.singletonList(SeedNode.create("192.168.132.234")));
    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, seedNodes);

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, provider.seedNodes().size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals("external", ctx.alternateAddress().get());
    environment.shutdown();
  }

  @Test
  void forceDefaultModeIfDefault() {
    Core core = mock(Core.class);
    CoreEnvironment environment = CoreEnvironment
      .builder()
      .ioConfig(IoConfig.networkResolution(NetworkResolution.DEFAULT))
      .build();

    CoreContext ctx = new CoreContext(core, 1, environment, PasswordAuthenticator.create("user", "pw"));
    when(core.context()).thenReturn(ctx);

    Set<SeedNode> seedNodes = new HashSet<>(Collections.singletonList(SeedNode.create("192.168.132.234")));
    DefaultConfigurationProvider provider = new DefaultConfigurationProvider(core, seedNodes);

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, provider.seedNodes().size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals(Optional.empty(), ctx.alternateAddress());
    environment.shutdown();
  }

}
