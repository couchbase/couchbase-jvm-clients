/*
 * Copyright (c) 2016 Couchbase, Inc.
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
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link KeyValueBucketRefresher}.
 *
 * <p>Note that the unit test covers the different error cases. In here we just make sure
 * that configs are loaded in the "good" cases.</p>
 */
class KeyValueBucketRefresherIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;

  /**
   * We are using a shorter config poll interval in this test to keep the test runtime
   * small.
   */
  @BeforeEach
  void beforeEach() {
    env = environment()
      .ioConfig(IoConfig.configPollInterval(Duration.ofSeconds(1)))
      .build();
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }

  @Test
  void pollsForNewConfigs() throws Exception {
    Core core = Core.create(env);

    ConfigurationProvider delegateProvider = new DefaultConfigurationProvider(core);
    ProposedBucketConfigInspectingProvider inspectingProvider = new ProposedBucketConfigInspectingProvider(delegateProvider);
    KeyValueBucketRefresher refresher = new KeyValueBucketRefresher(inspectingProvider, core);

    delegateProvider.openBucket(config().bucketname()).block();

    refresher.register(config().bucketname()).block();

    Util.waitUntilCondition(() -> inspectingProvider.timings.size() >= 2);

    long expected = env.ioConfig().configPollInterval().toNanos();

    assertTrue((inspectingProvider.timings.get(1) - inspectingProvider.timings.get(0)) > expected);

    refresher.deregister(config().bucketname()).block();

    long size = inspectingProvider.timings.size();
    Thread.sleep(env.ioConfig().configPollInterval().toMillis());
    assertEquals(size, inspectingProvider.timings.size());

    for (ProposedBucketConfigContext config : inspectingProvider.configs) {
      assertEquals(config().bucketname(), config.bucketName());
      assertNotNull(config.config());
    }

    refresher.shutdown().block();
    inspectingProvider.shutdown().block();
  }

  class ProposedBucketConfigInspectingProvider implements ConfigurationProvider {

    private final ConfigurationProvider delegate;

    private final List<ProposedBucketConfigContext> configs = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> timings = Collections.synchronizedList(new ArrayList<>());

    ProposedBucketConfigInspectingProvider(final ConfigurationProvider delegate) {
      this.delegate = delegate;
    }

    @Override
    public void proposeBucketConfig(final ProposedBucketConfigContext ctx) {
      timings.add(System.nanoTime());
      configs.add(ctx);
      delegate.proposeBucketConfig(ctx);
    }

    @Override
    public Flux<ClusterConfig> configs() {
      return delegate.configs();
    }

    @Override
    public ClusterConfig config() {
      return delegate.config();
    }

    @Override
    public Mono<Void> openBucket(String name) {
      return delegate.openBucket(name);
    }

    @Override
    public Mono<Void> closeBucket(String name) {
      return delegate.closeBucket(name);
    }

    @Override
    public Mono<Void> shutdown() {
      return delegate.shutdown();
    }

    @Override
    public void proposeGlobalConfig(ProposedGlobalConfigContext ctx) {
      delegate.proposeGlobalConfig(ctx);
    }

    @Override
    public Mono<Void> loadAndRefreshGlobalConfig() {
      return delegate.loadAndRefreshGlobalConfig();
    }

    @Override
    public CollectionMap collectionMap() {
      return delegate.collectionMap();
    }

    @Override
    public Mono<Void> refreshCollectionMap(String bucket, boolean force) {
      return delegate.refreshCollectionMap(bucket, force);
    }
  }
}
