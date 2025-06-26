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
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.config.CollectionMapRefreshFailedEvent;
import com.couchbase.client.core.cnc.events.config.CollectionMapRefreshIgnoredEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest;
import com.couchbase.client.core.msg.kv.GetCollectionIdResponse;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.topology.TopologyRevision;
import com.couchbase.client.core.util.ConnectionString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.readResource;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

class DefaultConfigurationProviderTest {

  private static CoreEnvironment ENVIRONMENT;
  private static SimpleEventBus EVENT_BUS;
  private static final String ORIGIN = "127.0.0.1";

  private DefaultConfigurationProvider provider;

  @BeforeAll
  static void setup() {
    EVENT_BUS = new SimpleEventBus(true);
    ENVIRONMENT = CoreEnvironment.builder().eventBus(EVENT_BUS).build();
  }

  @AfterAll
  static void teardown() {
    ENVIRONMENT.shutdown();
  }

  @BeforeEach
  void beforeEach() {
    EVENT_BUS.clear();
  }

  @AfterEach
  void afterEach() {
    close(provider);
  }

  private static DefaultConfigurationProvider newDefaultConfigurationProvider(Core core) {
    return newDefaultConfigurationProvider(core, ConnectionString.create("127.0.0.1"));
  }

  private static DefaultConfigurationProvider newDefaultConfigurationProvider(Core core, ConnectionString cs) {
    DefaultConfigurationProvider p = new DefaultConfigurationProvider(core, cs);
    // Ensures the provider is ready to start accepting new configs.
    // (This is only necessary in unit tests; normally all configs come from the server,
    // and the SDK can't connect to the server until after the seed nodes are resolved.)
    waitForSeedNodes(p);
    return p;
  }

  private static Set<SeedNode> waitForSeedNodes(ConfigurationProvider p) {
    return p.seedNodes().next().block(Duration.ofSeconds(30));
  }

  static void close(@Nullable ConfigurationProvider p) {
    try {
      if (p != null) {
        p.shutdown().block(Duration.ofSeconds(30));
      }
    } catch (AlreadyShutdownException ignore) {
    }
  }

  @Test
  void currentConfigIsReplayedToLateSubscriber() {
    Core core = mockCore(ENVIRONMENT);

    DefaultConfigurationProvider provider = newDefaultConfigurationProvider(core);
    provider.configs().blockFirst(Duration.ofSeconds(10));
  }

  @Test
  void canProposeNewBucketConfig() {
    Core core = mockCore(ENVIRONMENT);

    provider = newDefaultConfigurationProvider(core);

    final AtomicInteger configsPushed = new AtomicInteger(0);
    provider.configs()
        .skip(1) // ignore initial empty config
        .subscribe((c) -> configsPushed.incrementAndGet());

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, waitForSeedNodes(provider).size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));
    assertEquals(1, configsPushed.get());
    assertFalse(provider.config().bucketConfigs().isEmpty());

    ClusterTopology topology = provider.config().bucketTopology("default");
    assertNotNull(topology);

    assertEquals(new TopologyRevision(0, 1073), topology.revision());

    // No address in the default network matches a seed node, so the SDK should fall back to external network.
    assertEquals(NetworkResolution.EXTERNAL, topology.network());

    assertEquals(
      setOf(
        SeedNode.create("192.168.132.234").withKvPort(32775).withManagerPort(32790),
        SeedNode.create("192.168.132.234").withKvPort(32799).withManagerPort(32814),
        SeedNode.create("192.168.132.234").withKvPort(32823).withManagerPort(32838)
      ),
      getSeedNodesFromConfig(provider)
    );
  }

  @Test
  void ignoreProposedConfigWithLowerOrEqualRev() {
    Core core = mockCore(ENVIRONMENT);

    provider = newDefaultConfigurationProvider(core);

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
    Core core = mockCore(ENVIRONMENT);

    provider = newDefaultConfigurationProvider(core);

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
    Core core = mockCore(ENVIRONMENT);

    provider = newDefaultConfigurationProvider(core);

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
    assertEquals(2, configsPushed.get());

    assertTrue(provider.config().bucketConfigs().isEmpty());
  }

  /**
   * Makes sure that even if we only get a global config the seed nodes are properly updated from
   * that config.
   */
  @Test
  void updatesSeedNodesFromGlobalConfig() {
    Core core = mockCore(ENVIRONMENT);

    provider = newDefaultConfigurationProvider(core);

    String newConfig = readResource(
      "global_config_mad_hatter_multi_node.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeGlobalConfig(new ProposedGlobalConfigContext(newConfig, "127.0.0.1"));

    assertEquals(2, getSeedNodesFromConfig(provider).size());
    for (SeedNode sn : getSeedNodesFromConfig(provider)) {
      assertEquals(11210, sn.kvPort().orElse(null));
      assertEquals(8091, sn.clusterManagerPort().orElse(null));
      assertTrue(sn.address().equals("10.143.193.101") || sn.address().equals("10.143.193.102"));
    }
  }

  @Test
  void externalModeSelectedIfAuto() {
    Core core = mockCore(ENVIRONMENT);

    provider = newDefaultConfigurationProvider(core, ConnectionString.create("192.168.132.234:32790=manager"));

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, waitForSeedNodes(provider).size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals(
      setOf("192.168.132.234"),
      provider.config()
        .bucketConfig(bucket)
        .nodes()
        .stream().map(NodeInfo::hostname)
        .collect(toSet())
    );
  }

  @Test
  void forceDefaultModeIfDefault() {
    CoreEnvironment environment = CoreEnvironment.builder()
      .ioConfig(it -> it.networkResolution(NetworkResolution.DEFAULT))
      .build();
    Core core = mockCore(environment);

    DefaultConfigurationProvider provider = newDefaultConfigurationProvider(core, ConnectionString.create("192.168.132.234:32790=manager"));

    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(1, waitForSeedNodes(provider).size());

    String bucket = "default";
    String config = readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals(
      setOf("172.17.0.2", "172.17.0.3", "172.17.0.4"),
      provider.config()
        .bucketConfig(bucket)
        .nodes()
        .stream().map(NodeInfo::hostname)
        .collect(toSet())
    );

    environment.shutdown();
  }

  /**
   * Regression test for JVMCBC-880.
   * <p>
   * Verifies that when multiple bucket open attempts are happening in parallel, the bucketConfigLoadInProgress method
   * is not returning false prematurely (namely when only one is finished but one is still outstanding).
   */
  @Test
  void handlesMultipleBucketOpenInProgress() throws Exception {
    Core core = mockCore(ENVIRONMENT);

    Sinks.One<ProposedBucketConfigContext> bucket1Barrier = Sinks.one();
    Sinks.One<ProposedBucketConfigContext> bucket2Barrier = Sinks.one();

    ConfigurationProvider cp = new DefaultConfigurationProvider(core, ConnectionString.create("127.0.0.1")) {
      @Override
      protected Mono<ProposedBucketConfigContext> loadBucketConfigForSeed(NodeIdentifier identifier, int mappedKvPort,
                                                                          int mappedManagerPort, String name) {
        if (name.equals("bucket1")) {
          return bucket1Barrier.asMono();
        } else {
          return bucket2Barrier.asMono();
        }
      }

      @Override
      public void proposeBucketConfig(ProposedBucketConfigContext ctx) { }

      @Override
      protected Mono<Void> registerRefresher(String bucket) {
        return Mono.empty();
      }
    };
    waitForSeedNodes(cp);

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
      .subscribe(i -> bucket1Barrier.tryEmitValue(new ProposedBucketConfigContext("bucket1", "{}", "127.0.0.1")));
    Mono
      .delay(Duration.ofMillis(200))
      .subscribe(i -> bucket2Barrier.tryEmitValue(new ProposedBucketConfigContext("bucket2", "{}", "127.0.0.1")));

    assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  /**
   * It is allowed to have multiple attempts in-flight at the same time, but not for the same collection identifier
   * (since this would just spam the cluster unnecessarily).
   */
  @Test
  void ignoresMultipleCollectionIdRefreshAttempts() {
    Core core = mockCore(ENVIRONMENT);

    List<GetCollectionIdRequest> capturedRequests = new ArrayList<>();
    doAnswer(invocation -> {
      capturedRequests.add(invocation.getArgument(0));
      return null;
    }).when(core).send(any(GetCollectionIdRequest.class));

    provider = newDefaultConfigurationProvider(core);

    assertFalse(provider.collectionRefreshInProgress());

    CollectionIdentifier identifier1 = new CollectionIdentifier("bucket", Optional.of("scope"), Optional.of("collection"));
    CollectionIdentifier identifier2 = new CollectionIdentifier("bucket", Optional.of("_default"), Optional.of("_default"));

    provider.refreshCollectionId(identifier1);
    assertEquals(1, provider.collectionMapRefreshInProgress.size());
    assertTrue(provider.collectionMapRefreshInProgress.contains(identifier1));

    provider.refreshCollectionId(identifier2);
    assertEquals(2, provider.collectionMapRefreshInProgress.size());
    assertTrue(provider.collectionMapRefreshInProgress.contains(identifier2));

    provider.refreshCollectionId(identifier2);
    assertEquals(2, provider.collectionMapRefreshInProgress.size());
    assertTrue(provider.collectionMapRefreshInProgress.contains(identifier2));

    boolean found = false;
    for (Event event : EVENT_BUS.publishedEvents()) {
      if (event instanceof CollectionMapRefreshIgnoredEvent) {
        assertEquals(((CollectionMapRefreshIgnoredEvent) event).collectionIdentifier(), identifier2);
        found = true;
      }
    }
    assertTrue(found);

    capturedRequests.get(0).succeed(new GetCollectionIdResponse(ResponseStatus.SUCCESS, Optional.of(1234L)));
    assertTrue(provider.collectionRefreshInProgress());

    capturedRequests.get(1).cancel(CancellationReason.TIMEOUT);
    waitUntilCondition(() -> !provider.collectionRefreshInProgress());

    found = false;
    for (Event event : EVENT_BUS.publishedEvents()) {
      if (event instanceof CollectionMapRefreshFailedEvent) {
        assertEquals(((CollectionMapRefreshFailedEvent) event).collectionIdentifier(), identifier2);
        found = true;
      }
    }
    assertTrue(found);
  }

  @ParameterizedTest
  @CsvSource({
    "config_lower_rev_no_epoch,1,0,config_higher_rev_no_epoch,2,0",
    "config_lower_rev_no_epoch,1,0,config_lower_rev_higher_epoch,1,2",
    "config_lower_rev_lower_epoch,1,1,config_lower_rev_higher_epoch,1,2",
    "config_higher_rev_lower_epoch,2,1,config_lower_rev_higher_epoch,1,2",
  })
  void applyBucketConfigWithRevOrEpoch(String oldConfigFile, long oldRev, long oldEpoch,
                                       String newConfigFile, long newRev, long newEpoch) {
    Core core = mockCore(ENVIRONMENT);
    provider = newDefaultConfigurationProvider(core);
    String bucket = "travel-sample";

    String config = readResource(
      oldConfigFile + ".json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals(oldRev, provider.config().bucketConfig(bucket).rev());
    assertEquals(oldEpoch, provider.config().bucketConfig(bucket).revEpoch());

    String newConfig = readResource(
      newConfigFile + ".json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, newConfig, ORIGIN));

    assertEquals(newRev, provider.config().bucketConfig(bucket).rev());
    assertEquals(newEpoch, provider.config().bucketConfig(bucket).revEpoch());
  }

  @ParameterizedTest
  @CsvSource({
    "config_higher_rev_no_epoch,2,0,config_lower_rev_no_epoch",
    "config_lower_rev_lower_epoch,1,1,config_higher_rev_no_epoch",
    "config_higher_rev_lower_epoch,2,1,config_lower_rev_lower_epoch",
    "config_lower_rev_higher_epoch,1,2,config_higher_rev_lower_epoch",
  })
  void ignoresBucketConfigWithOlderRevOrEpoch(String oldConfigFile, long oldRev, long oldEpoch, String newConfigFile) {
    Core core = mockCore(ENVIRONMENT);
    provider = newDefaultConfigurationProvider(core);
    String bucket = "travel-sample";

    String config = readResource(
      oldConfigFile + ".json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, ORIGIN));

    assertEquals(oldRev, provider.config().bucketConfig(bucket).rev());
    assertEquals(oldEpoch, provider.config().bucketConfig(bucket).revEpoch());

    String newConfig = readResource(
      newConfigFile + ".json",
      DefaultConfigurationProviderTest.class
    );
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, newConfig, ORIGIN));

    assertEquals(oldRev, provider.config().bucketConfig(bucket).rev());
    assertEquals(oldEpoch, provider.config().bucketConfig(bucket).revEpoch());
  }

  private static Set<SeedNode> getSeedNodesFromConfig(ConfigurationProvider provider) {
    return provider.seedNodes().blockFirst(Duration.ZERO);
  }
}
