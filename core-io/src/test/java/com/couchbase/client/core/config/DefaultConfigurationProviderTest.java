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
import com.couchbase.client.core.io.NetworkAddress;
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

    String bucket = "default";
    String config = Utils.readResource(
      "config_with_external.json",
      DefaultConfigurationProviderTest.class
    );
    NetworkAddress origin = NetworkAddress.localhost();
    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, origin));
    assertEquals(1, configsPushed.get());
    assertFalse(provider.config().bucketConfigs().isEmpty());
    assertEquals(1073, provider.config().bucketConfig("default").rev());
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
    NetworkAddress origin = NetworkAddress.localhost();

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, origin));
    assertEquals(1, configsPushed.get());

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, origin));
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
    NetworkAddress origin = NetworkAddress.localhost();

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, origin));
    assertEquals(1, configsPushed.get());

    String newConfig = Utils.readResource(
      "config_with_external_higher_rev.json",
      DefaultConfigurationProviderTest.class
    );

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, newConfig, origin));
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
    NetworkAddress origin = NetworkAddress.localhost();

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, config, origin));
    assertEquals(1, configsPushed.get());

    String newConfig = Utils.readResource(
      "config_with_external_higher_rev.json",
      DefaultConfigurationProviderTest.class
    );

    provider.shutdown().block();

    provider.proposeBucketConfig(new ProposedBucketConfigContext(bucket, newConfig, origin));
    assertEquals(3, configsPushed.get());

    assertTrue(provider.config().bucketConfigs().isEmpty());
  }

}