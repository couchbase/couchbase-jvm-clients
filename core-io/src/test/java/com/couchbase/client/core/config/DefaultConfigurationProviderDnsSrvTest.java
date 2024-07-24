/*
 * Copyright (c) 2022 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.util.ConnectionString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.config.DefaultConfigurationProviderTest.mockCore;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Covers DNS SRV-specific tests for the {@link DefaultConfigurationProvider}.
 */
class DefaultConfigurationProviderDnsSrvTest {

  private static CoreEnvironment environment;
  private static SimpleEventBus eventBus;

  @BeforeAll
  static void setup() {
    eventBus = new SimpleEventBus(true);
    environment = CoreEnvironment.builder().eventBus(eventBus).build();
  }

  @AfterAll
  static void teardown() {
    environment.shutdown();
  }

  @BeforeEach
  void beforeEach() {
    eventBus.clear();
  }

  @Test
  void performsDnsSrvLookupOnSignal() {
    Core core = mockCore(environment);

    final AtomicBoolean called = new AtomicBoolean();
    ConfigurationProvider provider = new DefaultConfigurationProvider(
      core,
      emptySet(),
      ConnectionString.create("couchbase://myConnectionString")
    ) {
      @Override
      protected List<String> performDnsSrvLookup(boolean tlsEnabled) {
        called.set(true);
        return Collections.emptyList();
      }
    };

    provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
    waitUntilCondition(called::get);
  }

  @Test
  void respectsMinDnsLookupInterval() throws Exception {
    Core core = mockCore(environment);

    final AtomicInteger numCalled = new AtomicInteger();
    ConfigurationProvider provider = new DefaultConfigurationProvider(
      core,
      emptySet(),
      ConnectionString.create("couchbase://myConnectionString")
    ) {
      @Override
      protected List<String> performDnsSrvLookup(boolean tlsEnabled) {
        numCalled.incrementAndGet();
        return Collections.emptyList();
      }
    };

    for (int i = 0; i < 20; i++) {
      provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
      Thread.sleep(1);
    }
    assertEquals(1, numCalled.get());
  }

  @Test
  void ignoresSignalWithIneligibleConnectionString() throws Exception {
    Core core = mockCore(environment);

    final AtomicBoolean called = new AtomicBoolean();
    Set<SeedNode> seedNodes = setOf(SeedNode.create("1.2.3.4")); // DNS SRV requires hostname, not IP literal
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, seedNodes) {
      @Override
      protected List<String> performDnsSrvLookup(boolean tlsEnabled) {
        called.set(true);
        return Collections.emptyList();
      }
    };

    provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
    MILLISECONDS.sleep(250); // performDnsSrvLookup is called asynchronously; give it a chance to complete.
    assertFalse(called.get());
  }

  @Test
  void ignoresSignalIfDnsSrvDisabled() throws Exception {
    final CoreEnvironment environment = CoreEnvironment.builder()
      .ioConfig(io -> io.enableDnsSrv(false))
      .build();

    try {
      Core core = mockCore(environment);

      final AtomicBoolean called = new AtomicBoolean();
      ConfigurationProvider provider = new DefaultConfigurationProvider(
        core,
        emptySet(),
        ConnectionString.create("couchbase://foo")
      ) {
        @Override
        protected List<String> performDnsSrvLookup(boolean tlsEnabled) {
          called.set(true);
          return Collections.emptyList();
        }
      };

      provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
      MILLISECONDS.sleep(250); // performDnsSrvLookup is called asynchronously; give it a chance to complete.
      assertFalse(called.get());
    } finally {
      environment.shutdown();
    }
  }

}
