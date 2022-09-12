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
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    Core core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, environment, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);

    final AtomicBoolean called = new AtomicBoolean();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core,
      Collections.emptySet(), "couchbase://myConnectionString") {
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
    Core core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, environment, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);

    final AtomicInteger numCalled = new AtomicInteger();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core,
      Collections.emptySet(), "couchbase://myConnectionString") {
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
  void ignoresSignalWithNullConnectionString() {
    Core core = mock(Core.class);
    CoreContext ctx = new CoreContext(core, 1, environment, mock(Authenticator.class));
    when(core.context()).thenReturn(ctx);

    final AtomicBoolean called = new AtomicBoolean();
    ConfigurationProvider provider = new DefaultConfigurationProvider(core, Collections.emptySet()) {
      @Override
      protected List<String> performDnsSrvLookup(boolean tlsEnabled) {
        called.set(true);
        return Collections.emptyList();
      }
    };

    provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
    assertFalse(called.get());
  }

  @Test
  void ignoresSignalIfDnsSrvDisabled() {
    final CoreEnvironment environment = CoreEnvironment.builder().ioConfig(
      IoConfig.builder().enableDnsSrv(false)
    ).build();

    try {
      Core core = mock(Core.class);
      CoreContext ctx = new CoreContext(core, 1, environment, mock(Authenticator.class));
      when(core.context()).thenReturn(ctx);

      final AtomicBoolean called = new AtomicBoolean();
      ConfigurationProvider provider = new DefaultConfigurationProvider(core,
        Collections.emptySet(), "couchbase://foo") {
        @Override
        protected List<String> performDnsSrvLookup(boolean tlsEnabled) {
          called.set(true);
          return Collections.emptyList();
        }
      };

      provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
      assertFalse(called.get());
    } finally {
      environment.shutdown();
    }
  }

}
