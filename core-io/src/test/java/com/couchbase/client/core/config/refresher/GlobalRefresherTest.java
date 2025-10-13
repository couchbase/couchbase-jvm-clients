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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigRefreshFailure;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ConfigurationProvider.TopologyPollingTrigger;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierGlobalConfigRequest;
import com.couchbase.client.core.msg.kv.CarrierGlobalConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyBuilder;
import com.couchbase.client.core.util.Bytes;
import com.couchbase.client.core.util.NanoTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GlobalRefresherTest {

  private static final Duration FAST_CONFIG_POLL_INTERVAL = Duration.ofMillis(300);

  private CoreEnvironment env;
  private Core core;

  @BeforeEach
  void beforeEach() {
    SimpleEventBus eventBus = new SimpleEventBus(true);
    env = CoreEnvironment.builder()
      .eventBus(eventBus)
      .ioConfig(io -> io.configPollInterval(FAST_CONFIG_POLL_INTERVAL))
      .build();

    core = mockCore(env);
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }

  static ConfigurationProvider mockConfigurationProvider(ClusterConfig clusterConfig) {
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.config()).thenReturn(clusterConfig);
    when(provider.topologyPollingTriggers(any())).
      thenReturn(Flux.interval(FAST_CONFIG_POLL_INTERVAL).map(it -> TopologyPollingTrigger.TIMER));
    return provider;
  }

  @Test
  @SuppressWarnings("unchecked")
  void respectsPollInterval() {
    ClusterConfig clusterConfig = new ClusterConfig();
    ConfigurationProvider provider = mockConfigurationProvider(clusterConfig);
    ClusterTopology config = new ClusterTopologyBuilder()
      .addNode("foo", it -> it.ports(mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091)))
      .addNode("bar", it -> it.ports(mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091)))
      .build();

    clusterConfig.setGlobalConfig(config);

    final AtomicInteger invocationCounter = new AtomicInteger(0);

    doAnswer(invocation -> {
      invocationCounter.incrementAndGet();
      CarrierGlobalConfigRequest request = invocation.getArgument(0);

     CarrierGlobalConfigResponse response = mock(CarrierGlobalConfigResponse.class);
     when(response.status()).thenReturn(ResponseStatus.SUCCESS);
     when(response.content()).thenReturn(Bytes.EMPTY_BYTE_ARRAY);

     request.succeed(response);
     return null;
    }).when(core).send(isA(Request.class));

    GlobalRefresher refresher = new GlobalRefresher(provider, core) {
      @Override
      protected Duration pollerInterval() {
        return Duration.ofMillis(10);
      }
    };

    refresher.start().block();

    NanoTimestamp start = NanoTimestamp.now();
    waitUntilCondition(() -> invocationCounter.get() >= 3);

    // Note that it's 600 (*2) and not 900 (*3) for 3 attempts, since the first one does not count
    // towards the "last poll" time. The barrier really only starts working after the first
    // successful result, making sure that failed attempts to not count towards the poll time.
    assertTrue(start.elapsed().toMillis() >= FAST_CONFIG_POLL_INTERVAL.toMillis() * 2);

    refresher.shutdown().block();
  }

  @Test
  @SuppressWarnings("unchecked")
  void triggersEventIfAllNodesFailedToRefresh() {
    ClusterConfig clusterConfig = new ClusterConfig();
    ConfigurationProvider provider = mockConfigurationProvider(clusterConfig);
    ClusterTopology config = new ClusterTopologyBuilder()
      .addNode("foo", it -> it.ports(mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091)))
      .addNode("bar", it -> it.ports(mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091)))
      .build();

    clusterConfig.setGlobalConfig(config);

    final AtomicInteger invocationCounter = new AtomicInteger(0);

    doAnswer(invocation -> {
      invocationCounter.incrementAndGet();
      CarrierGlobalConfigRequest request = invocation.getArgument(0);

      CarrierGlobalConfigResponse response = mock(CarrierGlobalConfigResponse.class);
      when(response.content()).thenReturn(Bytes.EMPTY_BYTE_ARRAY);
      when(response.status()).thenReturn(ResponseStatus.NOT_FOUND);
      request.fail(new RuntimeException("Request Failed"));

      return null;
    }).when(core).send(isA(Request.class));

    GlobalRefresher refresher = new GlobalRefresher(provider, core) {
      @Override
      protected Duration pollerInterval() {
        return Duration.ofMillis(10);
      }
    };

    refresher.start().block();

    verify(provider, timeout(1000).atLeast(1))
      .signalConfigRefreshFailed(any(ConfigRefreshFailure.class));
  }

}
