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
import com.couchbase.client.core.node.NodeIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.couchbase.client.test.Util.readResource;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
    assertEquals(1, provider.currentSeedNodes().size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());
    assertFalse(provider.config().bucketConfigs().isEmpty());
    assertEquals(1073, provider.config().bucketConfig("default").rev());

    assertEquals(3, getSeedNodesFromConfig(provider).size());
    for (SeedNode node : getSeedNodesFromConfig(provider)) {
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

    assertEquals(2, getSeedNodesFromConfig(provider).size());
    for (SeedNode sn : getSeedNodesFromConfig(provider)) {
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
    assertEquals(1, provider.currentSeedNodes().size());

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
    assertEquals(1, provider.currentSeedNodes().size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals(Optional.empty(), ctx.alternateAddress());
    environment.shutdown();
  }

  /**
   * Regression test for JVMCBC-880.
   * <p>
   * Verifies that when multiple bucket open attempts happen in parallel, the bucketConfigLoadInProgress method
   * is not returning false prematurely (namely when only one is finished but one is still oustanding).
   */
  @Test
  void handlesMultipleBucketOpenInProgress() throws Exception {
    Core core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, ENVIRONMENT, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);
    Set<SeedNode> seedNodes = new HashSet<>(Collections.singletonList(SeedNode.create("127.0.0.1")));


    MonoProcessor<ProposedBucketConfigContext> bucket1Barrier = MonoProcessor.create();
    MonoProcessor<ProposedBucketConfigContext> bucket2Barrier = MonoProcessor.create();

    ConfigurationProvider cp = new DefaultConfigurationProvider(core, seedNodes) {
      @Override
      protected Mono<ProposedBucketConfigContext> loadBucketConfigForSeed(NodeIdentifier identifier, int mappedKvPort,
                                                                          int mappedManagerPort, String name,
                                                                          Optional<String> alternateAddress) {
        if (name.equals("bucket1")) {
          return bucket1Barrier;
        } else {
          return bucket2Barrier;
        }
      }

      @Override
      public void proposeBucketConfig(ProposedBucketConfigContext ctx) { }

      @Override
      protected Mono<Void> registerRefresher(String bucket) {
        return Mono.empty();
      }
    };
    assertFalse(cp.bucketConfigLoadInProgress());

    CountDownLatch latch = new CountDownLatch(2);
    cp.openBucket("bucket1").subscribe(unused -> {}, Assertions::fail, () -> {
      assertTrue(cp.bucketConfigLoadInProgress());
      latch.countDown();
    });
    cp.openBucket("bucket2").subscribe(unused -> {}, Assertions::fail, () -> {
      assertFalse(cp.bucketConfigLoadInProgress());
      latch.countDown();
    });

    // we pretend bucket 1 takes 1ms, while bucket2 takes 200ms
    Mono
      .delay(Duration.ofMillis(1))
      .subscribe(i -> bucket1Barrier.onNext(new ProposedBucketConfigContext("bucket1", "{}", "127.0.0.1")));
    Mono
      .delay(Duration.ofMillis(200))
      .subscribe(i -> bucket2Barrier.onNext(new ProposedBucketConfigContext("bucket2", "{}", "127.0.0.1")));

    latch.await(5, TimeUnit.SECONDS);
  }

  private static Set<SeedNode> getSeedNodesFromConfig(ConfigurationProvider provider) {
    return provider.seedNodes().blockFirst(Duration.ZERO);
  }
}
