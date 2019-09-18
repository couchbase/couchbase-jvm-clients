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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import com.couchbase.client.util.SimpleEventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the functionality of the {@link DefaultConfigurationProvider}.
 */
class DefaultConfigurationProviderIntegrationTest extends CoreIntegrationTest {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private CoreEnvironment environment;
  private Core core;

  @AfterEach
  void afterEach() {
    if (core != null) {
      core.shutdown().block();
    }
    if (environment != null) {
      environment.shutdown();
    }
  }

  /**
   * This is the simplest "good" case, just fetch a config that should be good to load.
   */
  @Test
  void openBucketAndCloseFromOneSeed() {
    TestNodeConfig cfg = config().firstNodeWith(Services.KV).get();
    Set<SeedNode> seeds = new HashSet<>(Collections.singletonList(SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV)),
      Optional.of(cfg.ports().get(Services.MANAGER))
    )));

    environment = CoreEnvironment.builder()
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);
    openAndClose(bucketName, provider);
    provider.shutdown().block();
  }

  /**
   * Bucket config should also be loaded when the second seed in the list is not available.
   */
  @Test
  void openBucketFromOneFirstValidSeed() {
    TestNodeConfig cfg = config().firstNodeWith(Services.KV).get();
    Set<SeedNode> seeds = new HashSet<>(Arrays.asList(
      SeedNode.create(
        cfg.hostname(),
        Optional.of(cfg.ports().get(Services.KV)),
        Optional.of(cfg.ports().get(Services.MANAGER))
      ),
      SeedNode.create("1.2.3.4")
    ));

    environment = CoreEnvironment.builder()
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);
    openAndClose(bucketName, provider);
    provider.shutdown().block();
  }

  /**
   * Bucket config should also be loaded when the first seed in the list is not available.
   */
  @Test
  void openBucketFromSecondValidSeed() {
    TestNodeConfig cfg = config().firstNodeWith(Services.KV).get();
    Set<SeedNode> seeds = new HashSet<>(Arrays.asList(
      SeedNode.create("1.2.3.4"),
      SeedNode.create(
        cfg.hostname(),
        Optional.of(cfg.ports().get(Services.KV)),
        Optional.of(cfg.ports().get(Services.MANAGER))
      )
    ));

    environment = CoreEnvironment.builder()
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);
    openAndClose(bucketName, provider);
    provider.shutdown().block();
  }

  /**
   * Error should be propagated when no good seed node is given.
   *
   * <p>Note that the timeout will be provided by the upper level, but if none is provided then
   * the natural timeout is dictated by the kv timeout + manager timeout as the maximum because
   * it performs the kv fetch and then the manager fetch as a fallback.</p>
   */
  @Test
  void propagateErrorFromInvalidSeedHostname() {
    Set<SeedNode> seeds = new HashSet<>(Collections.singletonList(SeedNode.create("1.2.3.4")));

    SimpleEventBus eventBus = new SimpleEventBus(true);
    environment = CoreEnvironment.builder()
      .eventBus(eventBus)
      .timeoutConfig(TimeoutConfig
        .kvTimeout(Duration.ofMillis(100))
        .managementTimeout(Duration.ofMillis(100))
      )
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);

    long start = System.nanoTime();
    assertThrows(ConfigException.class, () -> provider.openBucket(bucketName).block());
    long end = System.nanoTime();

    assertTrue(TimeUnit.NANOSECONDS.toMillis(end - start) >= 200);
    provider.shutdown().block();
  }

  /**
   * Error should be propagated when the seed node is good but the port is not.
   */
  @Disabled
  void propagateErrorFromInvalidSeedPort() {
    TestNodeConfig cfg = config().firstNodeWith(Services.KV).get();
    Set<SeedNode> seeds = new HashSet<>(Collections.singletonList(SeedNode.create(
      cfg.hostname(),
      Optional.of(9999),
      Optional.of(9998)
    )));

    environment = CoreEnvironment.builder()
      .timeoutConfig(TimeoutConfig
        .kvTimeout(Duration.ofMillis(500))
        .managementTimeout(Duration.ofMillis(500))
      )
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);

    long start = System.nanoTime();
    assertThrows(ConfigException.class, () -> provider.openBucket(bucketName).block());
    long end = System.nanoTime();

    assertTrue(TimeUnit.NANOSECONDS.toSeconds(end - start) >= 1);
    provider.shutdown().block();
  }

  /**
   * Perform the shutdown and checks that afterwards the other ops don't work anymore.
   */
  @Test
  void performShutdown() throws Exception {
    TestNodeConfig cfg = config().firstNodeWith(Services.KV).get();
    Set<SeedNode> seeds = new HashSet<>(Collections.singletonList(SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV)),
      Optional.of(cfg.ports().get(Services.MANAGER))
    )));

    environment = CoreEnvironment.builder()
      .timeoutConfig(TimeoutConfig
        .kvTimeout(Duration.ofSeconds(1))
        .managementTimeout(Duration.ofSeconds(1))
      )
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);

    AtomicInteger configEvents = new AtomicInteger(0);
    CountDownLatch configsComplete = new CountDownLatch(1);
    provider.configs().subscribe(
      (c) -> configEvents.incrementAndGet(),
      (e) -> {},
      configsComplete::countDown
    );

    assertEquals(0, provider.config().bucketConfigs().size());
    provider.openBucket(bucketName).timeout(DEFAULT_TIMEOUT).block();
    assertEquals(1, provider.config().bucketConfigs().size());

    provider.shutdown().timeout(DEFAULT_TIMEOUT).block();
    assertEquals(0, provider.config().bucketConfigs().size());

    assertThrows(AlreadyShutdownException.class, () -> provider.shutdown().block());
    assertThrows(AlreadyShutdownException.class, () -> provider.openBucket("foo").block());
    assertThrows(AlreadyShutdownException.class, () -> provider.closeBucket("foo").block());

    configsComplete.await(1, TimeUnit.SECONDS);
    assertEquals(3, configEvents.get());
  }

  /**
   * Helper method to perform a good open and close bucket sequence.
   *
   * <p>This also makes sure that the config stream is published with the configs.</p>
   *
   * @param bucketName name of the bucket.
   * @param provider the config provider to use.
   */
  private void openAndClose(final String bucketName, final ConfigurationProvider provider) {
    List<ClusterConfig> configs = Collections.synchronizedList(new ArrayList<>());
    provider.configs().subscribe(configs::add);

    assertTrue(provider.config().bucketConfigs().isEmpty());

    provider.openBucket(bucketName).timeout(DEFAULT_TIMEOUT).block();

    assertEquals(1, provider.config().bucketConfigs().size());
    assertEquals(
      bucketName,
      provider.config().bucketConfig(bucketName).name()
    );

    provider.closeBucket(bucketName).timeout(DEFAULT_TIMEOUT).block();
    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(2, configs.size());
    assertEquals(0, configs.get(1).bucketConfigs().size());
    assertTrue(configs.get(1).bucketConfigs().isEmpty());
  }

}
