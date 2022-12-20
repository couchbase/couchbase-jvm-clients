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
import com.couchbase.client.core.cnc.events.config.BucketOpenRetriedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.BucketNotFoundDuringLoadException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.util.ConfigWaitHelper;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterEach;
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

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the {@link DefaultConfigurationProvider}.
 */
class DefaultConfigurationProviderIntegrationTest extends CoreIntegrationTest {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private CoreEnvironment environment;
  private Core core;
  private ConfigWaitHelper configWaitHelper;

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


    SimpleEventBus eventBus = new SimpleEventBus(true);
    environment = CoreEnvironment.builder()
      .eventBus(eventBus)
      .timeoutConfig(TimeoutConfig.connectTimeout(Duration.ofMillis(500)))
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);
    openAndClose(bucketName, provider);
    provider.shutdown().block();

    waitUntilCondition(() -> eventBus.publishedEvents().stream().anyMatch(e -> e instanceof EndpointConnectionFailedEvent));
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

    SimpleEventBus eventBus = new SimpleEventBus(true);
    environment = CoreEnvironment.builder()
      .eventBus(eventBus)
      .timeoutConfig(TimeoutConfig.connectTimeout(Duration.ofMillis(500)))
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);
    openAndClose(bucketName, provider);
    provider.shutdown().block();

    waitUntilCondition(() -> eventBus.publishedEvents().stream().anyMatch(e -> e instanceof EndpointConnectionFailedEvent));
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
        .kvTimeout(kvTimeout)
        .managementTimeout(managementTimeout)
      )
      .build();
    core = Core.create(environment, authenticator(), seeds);

    String bucketName = config().bucketname();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);

    AtomicInteger configEvents = new AtomicInteger(0);
    CountDownLatch configsComplete = new CountDownLatch(1);
    provider.configs().skip(1).subscribe( // ignore initial empty config
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
    assertThrows(AlreadyShutdownException.class, () -> provider.closeBucket("foo", true).block());

    assertTrue(configsComplete.await(1, TimeUnit.SECONDS));
    assertEquals(2, configEvents.get());
  }

  /**
   * Need to make sure that if a bucket is not found during load, we continue retrying the open
   * bucket attempts.
   */
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void retriesOnBucketNotFoundDuringLoadException() {
    TestNodeConfig cfg = config().firstNodeWith(Services.KV).get();
    Set<SeedNode> seeds = new HashSet<>(Collections.singletonList(SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV)),
      Optional.of(cfg.ports().get(Services.MANAGER))
    )));
    SimpleEventBus eventBus = new SimpleEventBus(true);

    environment = CoreEnvironment.builder()
      .eventBus(eventBus)
      .build();
    configWaitHelper = new ConfigWaitHelper(environment.eventBus());
    core = Core.create(environment, authenticator(), seeds);
    configWaitHelper.await();

    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seeds);

    try {
      String bucketName = "this-bucket-does-not-exist";
      provider.openBucket(bucketName).subscribe(v -> {}, e -> assertInstanceOf(ConfigException.class, e));

      waitUntilCondition(() -> eventBus.publishedEvents().stream().anyMatch(p -> p instanceof BucketOpenRetriedEvent));

      for (Event event : eventBus.publishedEvents()) {
        if (event instanceof BucketOpenRetriedEvent) {
          assertEquals(bucketName, ((BucketOpenRetriedEvent) event).bucketName());
          assertInstanceOf(BucketNotFoundDuringLoadException.class, event.cause());
        }
      }
    } finally {
      provider.shutdown().block();
    }
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
    provider.configs()
        .skip(1) // ignore initial empty config
        .subscribe(configs::add);

    assertTrue(provider.config().bucketConfigs().isEmpty());

    provider.openBucket(bucketName).timeout(DEFAULT_TIMEOUT).block();

    assertEquals(1, provider.config().bucketConfigs().size());
    assertEquals(
      bucketName,
      provider.config().bucketConfig(bucketName).name()
    );

    provider.closeBucket(bucketName, true).timeout(DEFAULT_TIMEOUT).block();
    assertTrue(provider.config().bucketConfigs().isEmpty());
    assertEquals(2, configs.size());
    assertEquals(0, configs.get(1).bucketConfigs().size());
    assertTrue(configs.get(1).bucketConfigs().isEmpty());
  }

}
